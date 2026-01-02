const WebSocket = require('ws');
const { TranscribeStreamingClient, StartStreamTranscriptionCommand } = require('@aws-sdk/client-transcribe-streaming');
const express = require('express');
const admin = require('firebase-admin');
const algoliasearch = require('algoliasearch');
const crypto = require('crypto');
const { Readable } = require('stream');

// Initialize Firebase from environment variable
const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || '{}');
if (Object.keys(serviceAccount).length > 0) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
} else {
  admin.initializeApp();
}

const db = admin.firestore();
const app = express();
const PORT = process.env.PORT || 10000;

// ========== CREDENTIALS MANAGER ==========
let transcribeClient = null;
let credentialsCache = {
  aws: null,
  algolia: null,
  lastFetched: 0
};

async function getCredentials() {
  const now = Date.now();
  const CACHE_DURATION = 5 * 60 * 1000;
  
  if (credentialsCache.aws && credentialsCache.algolia && 
      (now - credentialsCache.lastFetched) < CACHE_DURATION) {
    return credentialsCache;
  }
  
  try {
    const doc = await db.collection('bible').doc('credentials').get();
    
    if (!doc.exists) {
      throw new Error('Credentials document not found in Firestore');
    }
    
    const data = doc.data();
    
    const requiredAWS = ['accessKey', 'secretKey'];
    const requiredAlgolia = ['appId', 'adminKey'];
    
    for (const field of requiredAWS) {
      if (!data[field]) {
        throw new Error(`Missing AWS ${field} in Firestore`);
      }
    }
    
    for (const field of requiredAlgolia) {
      if (!data[field]) {
        throw new Error(`Missing Algolia ${field} in Firestore`);
      }
    }
    
    credentialsCache = {
      aws: {
        accessKeyId: data.accessKey,
        secretAccessKey: data.secretKey,
        region: data.region || 'us-east-1'
      },
      algolia: {
        appId: data.appId,
        adminKey: data.adminKey
      },
      lastFetched: now
    };
    
    console.log('✅ Credentials loaded from Firestore');
    return credentialsCache;
    
  } catch (error) {
    console.error('❌ Error loading credentials:', error);
    throw error;
  }
}

async function getTranscribeClient() {
  if (!transcribeClient) {
    const credentials = await getCredentials();
    transcribeClient = new TranscribeStreamingClient({
      region: credentials.aws.region,
      credentials: credentials.aws,
    });
  }
  return transcribeClient;
}

async function getAlgoliaIndexForVersion(version = 'kjv') {
  const BIBLE_VERSIONS = {
    'kjv': { name: 'King James Version', searchIndex: 'bible_kjv' },
    'niv': { name: 'New International Version', searchIndex: 'bible_niv' },
    'esv': { name: 'English Standard Version', searchIndex: 'bible_esv' },
    'nlt': { name: 'New Living Translation', searchIndex: 'bible_nlt' },
    'nasb': { name: 'New American Standard Bible', searchIndex: 'bible_nasb' },
    'hausa': { name: 'Hausa Bible', searchIndex: 'bible_hausa' }
  };
  
  const indexName = BIBLE_VERSIONS[version]?.searchIndex || 'bible_kjv';
  const credentials = await getCredentials();
  const algoliaClient = algoliasearch(
    credentials.algolia.appId,
    credentials.algolia.adminKey
  );
  return algoliaClient.initIndex(indexName);
}

// ========== BIBLE VERSION DETECTION ==========
function detectBibleVersion(text) {
  const versionPatterns = {
    'niv': /\b(niv|new international version)\b/i,
    'esv': /\b(esv|english standard version)\b/i,
    'kjv': /\b(kjv|king james version|king james)\b/i,
    'nlt': /\b(nlt|new living translation)\b/i,
    'nasb': /\b(nasb|new american standard bible)\b/i,
    'hausa': /\b(hausa|hausa bible)\b/i
  };
  
  for (const [version, pattern] of Object.entries(versionPatterns)) {
    if (pattern.test(text)) {
      console.log(`✅ Detected Bible version: ${version}`);
      return version;
    }
  }
  
  return null;
}

// ========== VERSE REFERENCE DETECTION ==========
const BOOK_ABBREVIATIONS = {
  'gen': 'Genesis', 'exo': 'Exodus', 'lev': 'Leviticus', 'num': 'Numbers',
  'deu': 'Deuteronomy', 'jos': 'Joshua', 'judg': 'Judges', 'rut': 'Ruth',
  '1sa': '1 Samuel', '2sa': '2 Samuel', '1ki': '1 Kings', '2ki': '2 Kings',
  '1ch': '1 Chronicles', '2ch': '2 Chronicles', 'ezr': 'Ezra', 'neh': 'Nehemiah',
  'est': 'Esther', 'job': 'Job', 'psa': 'Psalms', 'ps': 'Psalms',
  'pro': 'Proverbs', 'ecc': 'Ecclesiastes', 'sng': 'Song of Solomon',
  'isa': 'Isaiah', 'jer': 'Jeremiah', 'lam': 'Lamentations',
  'ezk': 'Ezekiel', 'dan': 'Daniel', 'hos': 'Hosea', 'jol': 'Joel',
  'amo': 'Amos', 'obd': 'Obadiah', 'jon': 'Jonah', 'mic': 'Micah',
  'nam': 'Nahum', 'hab': 'Habakkuk', 'zep': 'Zephaniah', 'hag': 'Haggai',
  'zec': 'Zechariah', 'mal': 'Malachi',
  'mat': 'Matthew', 'mrk': 'Mark', 'luk': 'Luke', 'jhn': 'John',
  'act': 'Acts', 'rom': 'Romans', '1co': '1 Corinthians', '2co': '2 Corinthians',
  'gal': 'Galatians', 'eph': 'Ephesians', 'php': 'Philippians',
  'col': 'Colossians', '1th': '1 Thessalonians', '2th': '2 Thessalonians',
  '1ti': '1 Timothy', '2ti': '2 Timothy', 'tit': 'Titus', 'phm': 'Philemon',
  'heb': 'Hebrews', 'jas': 'James', '1pe': '1 Peter', '2pe': '2 Peter',
  '1jn': '1 John', '2jn': '2 John', '3jn': '3 John', 'jud': 'Jude',
  'rev': 'Revelation'
};

function detectVerseReference(text) {
  const fullBookPattern = /(\d?\s?\w+)\s+(\d+):(\d+)(?:-(\d+))?/gi;
  const abbrevPattern = /\b(gen|exo|lev|num|deu|jos|judg|rut|1sa|2sa|1ki|2ki|1ch|2ch|ezr|neh|est|job|psa|ps|pro|ecc|sng|isa|jer|lam|ezk|dan|hos|jol|amo|obd|jon|mic|nam|hab|zep|hag|zec|mal|mat|mrk|luk|jhn|act|rom|1co|2co|gal|eph|php|col|1th|2th|1ti|2ti|tit|phm|heb|jas|1pe|2pe|1jn|2jn|3jn|jud|rev)\b\s+(\d+):(\d+)(?:-(\d+))?/gi;
  
  const fullMatches = [...text.matchAll(fullBookPattern)];
  for (const match of fullMatches) {
    const book = match[1].trim();
    const chapter = parseInt(match[2]);
    const verse = parseInt(match[3]);
    const endVerse = match[4] ? parseInt(match[4]) : null;
    
    if (chapter > 0 && chapter < 151 && verse > 0 && verse < 177) {
      return {
        book: book,
        chapter: chapter,
        verse: verse,
        endVerse: endVerse,
        fullReference: match[0],
        type: 'direct_reference',
        confidence: 'high'
      };
    }
  }
  
  const abbrevMatches = [...text.matchAll(abbrevPattern)];
  for (const match of abbrevMatches) {
    const abbrev = match[1].toLowerCase();
    const fullBook = BOOK_ABBREVIATIONS[abbrev];
    const chapter = parseInt(match[2]);
    const verse = parseInt(match[3]);
    const endVerse = match[4] ? parseInt(match[4]) : null;
    
    if (fullBook && chapter > 0 && chapter < 151 && verse > 0 && verse < 177) {
      return {
        book: fullBook,
        chapter: chapter,
        verse: verse,
        endVerse: endVerse,
        fullReference: match[0],
        type: 'direct_reference',
        confidence: 'high'
      };
    }
  }
  
  return null;
}

// ========== TRIGGER PHRASE DETECTION ==========
const TRIGGER_PHRASES = [
  'the bible says',
  'scripture says', 
  'as it is written',
  'the word of god says',
  'according to the bible',
  'in the book of',
  'jesus said',
  'paul wrote',
  'psalm says',
  'the lord says',
  'god said'
];

function detectTriggerPhrase(text) {
  const normalized = text.toLowerCase();
  
  for (const trigger of TRIGGER_PHRASES) {
    if (normalized.includes(trigger)) {
      const triggerIndex = normalized.indexOf(trigger);
      const afterTrigger = text.substring(triggerIndex + trigger.length).trim();
      
      const cleaned = afterTrigger
        .replace(/^[\s,.-]+/, '')
        .replace(/[\s,.-]+$/, '')
        .trim();
      
      const words = cleaned.split(/\s+/).filter(w => w.length > 2);
      
      if (cleaned.length >= 8 && words.length >= 2) {
        return {
          trigger: trigger,
          query: cleaned,
          confidence: 'high'
        };
      }
    }
  }
  
  return null;
}

// ========== SEARCH FUNCTIONS ==========
async function searchExactVerse(sessionId, verseRef, version, ws) {
  try {
    const index = await getAlgoliaIndexForVersion(version);
    
    let searchQuery = `book:"${verseRef.book}" AND chapter:${verseRef.chapter}`;
    
    if (verseRef.endVerse) {
      searchQuery += ` AND verse:${verseRef.verse} TO ${verseRef.endVerse}`;
    } else {
      searchQuery += ` AND verse:${verseRef.verse}`;
    }
    
    console.log(`🔍 Searching exact verse: ${searchQuery}`);
    
    const { hits } = await index.search('', {
      filters: searchQuery,
      hitsPerPage: verseRef.endVerse ? (verseRef.endVerse - verseRef.verse + 1) : 1,
      attributesToRetrieve: ['*']
    });
    
    if (hits.length > 0) {
      const passages = formatSearchResults(hits);
      
      ws.send(JSON.stringify({
        type: 'verse_reference_found',
        reference: verseRef.fullReference,
        book: verseRef.book,
        chapter: verseRef.chapter,
        startVerse: verseRef.verse,
        endVerse: verseRef.endVerse || verseRef.verse,
        version: version,
        passages: passages,
        timestamp: Date.now()
      }));
      
    } else {
      ws.send(JSON.stringify({
        type: 'verse_not_found',
        reference: verseRef.fullReference,
        message: `Could not find ${verseRef.fullReference} in ${version.toUpperCase()}`,
        timestamp: Date.now()
      }));
    }
    
  } catch (error) {
    console.error('❌ Exact verse search error:', error);
    ws.send(JSON.stringify({
      type: 'error',
      message: `Could not find ${verseRef.fullReference}`,
      timestamp: Date.now()
    }));
  }
}

function formatSearchResults(hits) {
  const grouped = {};
  
  hits.forEach(hit => {
    const key = `${hit.book}_${hit.chapter}`;
    if (!grouped[key]) {
      grouped[key] = {
        reference: `${hit.book} ${hit.chapter}`,
        book: hit.book,
        chapter: hit.chapter,
        verses: []
      };
    }
    
    grouped[key].verses.push({
      verse: hit.verse,
      text: hit.text,
      highlighted: hit._highlightResult?.text?.value || hit.text
    });
  });
  
  Object.values(grouped).forEach(group => {
    group.verses.sort((a, b) => a.verse - b.verse);
  });
  
  return Object.values(grouped).slice(0, 3);
}

function prepareAlgoliaQuery(query) {
  const stopWords = new Set(['the', 'and', 'but', 'or', 'a', 'an', 'is', 'are', 'was', 'were']);
  
  const words = query.toLowerCase()
    .replace(/[^\w\s']/g, ' ')
    .split(/\s+/)
    .filter(word => word.length > 2 && !stopWords.has(word))
    .slice(0, 6);
  
  if (words.length === 0) return query;
  
  if (words.length >= 3) {
    return `"${words.join(' ')}"`;
  }
  
  return words.map(word => `"${word}"`).join(' AND ');
}

// ========== ACTIVE SESSIONS ==========
const activeSessions = new Map();

// ========== HELPER FUNCTIONS ==========
function sendError(sessionId, errorMessage, ws) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({
      type: 'error',
      message: errorMessage,
      timestamp: Date.now()
    }));
  }
}

async function handleAudioChunk(sessionId, audioChunk, ws) {
  const session = activeSessions.get(sessionId);
  if (!session) return;

  try {
    if (!session.audioStream) {
      await initializeAWSStream(sessionId, ws);
    }

    const audioBuffer = Buffer.from(audioChunk, 'base64');
    
    // Send audio chunk to AWS Transcribe
    if (session.writeAudioChunk) {
      session.writeAudioChunk(audioBuffer);
    }

  } catch (error) {
    console.error('❌ Error handling audio:', error);
    sendError(sessionId, 'Audio processing failed', ws);
  }
}

async function initializeAWSStream(sessionId, ws) {
  const session = activeSessions.get(sessionId);
  if (!session) return;

  try {
    const transcribeClient = await getTranscribeClient();
    
    // Create a proper Readable stream for AWS Transcribe
    let audioBufferCallback;
    const audioStreamGenerator = async function* () {
      while (true) {
        const chunk = await new Promise(resolve => {
          audioBufferCallback = resolve;
        });
        if (chunk === null) break; // End stream signal
        yield {
          AudioEvent: {
            AudioChunk: chunk
          }
        };
      }
    };

    const audioStream = Readable.from(audioStreamGenerator());

    const command = new StartStreamTranscriptionCommand({
      LanguageCode: session.language || 'en-US',
      MediaEncoding: 'pcm',
      MediaSampleRateHertz: 16000,
      EnablePartialResultsStabilization: true,
      PartialResultsStability: 'medium',
      AudioStream: audioStream
    });

    const response = await transcribeClient.send(command);
    session.audioStream = response;
    
    // Store the write function for audio chunks
    session.writeAudioChunk = (chunk) => {
      if (audioBufferCallback) {
        audioBufferCallback(chunk);
      }
    };

    response.TranscriptResultStream.on('data', async (event) => {
      await processTranscriptionResult(sessionId, event, ws);
    });

    response.TranscriptResultStream.on('error', (error) => {
      console.error('❌ AWS stream error:', error);
      sendError(sessionId, 'Transcription stream error', ws);
    });

    response.TranscriptResultStream.on('end', () => {
      console.log('✅ AWS Transcribe stream ended');
    });

    ws.send(JSON.stringify({
      type: 'status',
      message: 'Transcription started',
      timestamp: Date.now()
    }));

  } catch (error) {
    console.error('❌ AWS initialization error:', error);
    
    let errorMessage = `Failed to start transcription: ${error.message}`;
    
    if (error.name === 'InvalidSignatureException') {
      errorMessage = 'AWS credentials invalid. Check accessKey/secretKey in Firestore.';
    } else if (error.message.includes('Eventstream payload')) {
      errorMessage = 'AWS Transcribe stream configuration error. Check server logs.';
    }
    
    sendError(sessionId, errorMessage, ws);
    throw error;
  }
}

async function processTranscriptionResult(sessionId, event, ws) {
  const session = activeSessions.get(sessionId);
  if (!session) return;

  try {
    const results = event.TranscriptEvent?.Transcript?.Results;
    
    if (results && results.length > 0) {
      const result = results[0];
      const transcript = result.Alternatives[0]?.Transcript || '';
      
      if (transcript) {
        session.transcriptBuffer += ' ' + transcript;
        
        const words = session.transcriptBuffer.split(/\s+/);
        if (words.length > 200) {
          session.transcriptBuffer = words.slice(-200).join(' ');
        }
        
        const detectedVersion = detectBibleVersion(session.transcriptBuffer);
        if (detectedVersion && detectedVersion !== session.currentVersion) {
          session.currentVersion = detectedVersion;
          ws.send(JSON.stringify({
            type: 'version_changed',
            from: 'KJV',
            to: detectedVersion.toUpperCase(),
            version: detectedVersion,
            timestamp: Date.now()
          }));
        }
        
        if (!result.IsPartial) {
          ws.send(JSON.stringify({
            type: 'transcript',
            text: transcript,
            isPartial: false,
            timestamp: Date.now()
          }));
          
          const verseRef = detectVerseReference(session.transcriptBuffer);
          if (verseRef && (Date.now() - session.lastVerseReferenceTime > 3000)) {
            session.lastVerseReferenceTime = Date.now();
            console.log(`📖 Direct verse reference detected: ${verseRef.fullReference}`);
            await searchExactVerse(sessionId, verseRef, session.currentVersion, ws);
          }
          
          const trigger = detectTriggerPhrase(session.transcriptBuffer);
          if (trigger && (Date.now() - session.lastTriggerTime > 5000)) {
            session.lastTriggerTime = Date.now();
            console.log(`🔔 Trigger: "${trigger.trigger}", Query: "${trigger.query}"`);
            
            ws.send(JSON.stringify({
              type: 'trigger_detected',
              trigger: trigger.trigger,
              query: trigger.query,
              version: session.currentVersion,
              timestamp: Date.now()
            }));
            
            try {
              const index = await getAlgoliaIndexForVersion(session.currentVersion);
              const searchQuery = prepareAlgoliaQuery(trigger.query);
              
              const { hits } = await index.search(searchQuery, {
                hitsPerPage: 10,
                attributesToRetrieve: ['*'],
                attributesToHighlight: ['text']
              });
              
              if (hits.length > 0) {
                const passages = formatSearchResults(hits);
                
                ws.send(JSON.stringify({
                  type: 'passages_found',
                  trigger: trigger.trigger,
                  query: trigger.query,
                  version: session.currentVersion,
                  passages: passages,
                  timestamp: Date.now()
                }));
              }
            } catch (error) {
              console.error('❌ Search error:', error);
            }
          }
        } else {
          ws.send(JSON.stringify({
            type: 'partial_transcript',
            text: transcript,
            isPartial: true,
            timestamp: Date.now()
          }));
        }
      }
    }
  } catch (error) {
    console.error('❌ Transcription processing error:', error);
  }
}

async function handleControlMessage(sessionId, data, ws) {
  const session = activeSessions.get(sessionId);
  if (!session) return;
  
  switch (data.action) {
    case 'stop':
      await cleanupSession(sessionId);
      break;
    case 'change_version':
      if (data.version) {
        session.currentVersion = data.version;
        ws.send(JSON.stringify({
          type: 'version_changed',
          from: 'KJV',
          to: data.version.toUpperCase(),
          version: data.version,
          timestamp: Date.now()
        }));
      }
      break;
  }
}

async function cleanupSession(sessionId) {
  const session = activeSessions.get(sessionId);
  if (!session) return;
  
  // Send end signal to AWS stream
  if (session.writeAudioChunk) {
    session.writeAudioChunk(null);
  }
  
  if (session.audioStream) {
    try {
      session.audioStream.destroy();
    } catch (error) {
      console.error('❌ Error destroying AWS stream:', error);
    }
  }
  
  if (session.ws && session.ws.readyState === WebSocket.OPEN) {
    session.ws.close();
  }
  
  activeSessions.delete(sessionId);
  
  try {
    await db.collection('transcription_sessions').doc(sessionId).update({
      status: 'closed',
      closedAt: admin.firestore.FieldValue.serverTimestamp(),
      duration: Date.now() - session.startTime,
      finalVersion: session.currentVersion,
      versionHistory: session.versionHistory || ['kjv']
    });
    
    await db.collection('bible').doc('credentials').update({
      transcriptUrl: admin.firestore.FieldValue.delete(),
      activeSessionId: admin.firestore.FieldValue.delete()
    });
    
  } catch (error) {
    console.error('❌ Error updating session:', error);
  }
  
  console.log(`✅ Session cleaned up: ${sessionId}`);
}

// ========== WEB SOCKET SERVER ==========
const wss = new WebSocket.Server({ noServer: true });

wss.on('connection', async (ws, req) => {
  const sessionId = req.url.split('/').pop() || 'default-session';
  console.log(`✅ WebSocket connected: ${sessionId}`);
  
  try {
    const sessionDoc = await db.collection('transcription_sessions').doc(sessionId).get();
    
    if (!sessionDoc.exists) {
      ws.send(JSON.stringify({
        type: 'error',
        message: 'Session not found',
        timestamp: Date.now()
      }));
      ws.close(1008, 'Session not found');
      return;
    }

    const sessionData = sessionDoc.data();
    
    activeSessions.set(sessionId, {
      ws: ws,
      userId: sessionData.userId || 'anonymous',
      language: sessionData.language || 'en-US',
      currentVersion: 'kjv',
      startTime: Date.now(),
      audioStream: null,
      writeAudioChunk: null,
      transcriptBuffer: '',
      lastTriggerTime: 0,
      lastVerseReferenceTime: 0,
      versionHistory: ['kjv']
    });

    await db.collection('transcription_sessions').doc(sessionId).update({
      status: 'active',
      connectedAt: admin.firestore.FieldValue.serverTimestamp()
    });

    ws.on('message', async (message) => {
      try {
        const data = JSON.parse(message.toString());
        
        if (data.type === 'audio_chunk') {
          await handleAudioChunk(sessionId, data.chunk, ws);
        } else if (data.type === 'control') {
          await handleControlMessage(sessionId, data, ws);
        } else if (data.type === 'heartbeat') {
          await db.collection('transcription_sessions')
            .doc(sessionId)
            .update({
              lastActivity: admin.firestore.FieldValue.serverTimestamp()
            });
        }
      } catch (error) {
        console.error('❌ Error processing message:', error);
      }
    });

    ws.on('close', async () => {
      console.log(`🔌 WebSocket closed: ${sessionId}`);
      await cleanupSession(sessionId);
    });

    ws.on('error', (error) => {
      console.error(`❌ WebSocket error ${sessionId}:`, error);
    });

    ws.send(JSON.stringify({
      type: 'status',
      message: 'Connected to Bible Ear transcription',
      defaultVersion: 'KJV',
      timestamp: Date.now(),
      features: ['Direct verse references', 'Trigger phrases', 'Multiple Bible versions']
    }));
    
  } catch (error) {
    console.error('❌ Session setup error:', error);
    ws.send(JSON.stringify({
      type: 'error',
      message: 'Failed to setup session',
      timestamp: Date.now()
    }));
    ws.close(1011, 'Internal server error');
  }
});

// ========== HTTP SERVER ==========
const server = app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`🔗 WebSocket endpoint: ws://localhost:${PORT}/transcriptionWebSocket/{sessionId}`);
});

// Handle WebSocket upgrades
server.on('upgrade', (request, socket, head) => {
  const pathname = request.url;
  
  if (pathname.startsWith('/transcriptionWebSocket')) {
    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('connection', ws, request);
    });
  } else {
    socket.destroy();
  }
});

// Health check endpoints
app.get('/', (req, res) => {
  res.json({
    service: 'Bible Ear WebSocket Server',
    status: 'running',
    endpoints: {
      websocket: '/transcriptionWebSocket/{sessionId}',
      health: '/health'
    },
    active_sessions: activeSessions.size,
    uptime: process.uptime()
  });
});

app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    active_sessions: activeSessions.size,
    memory: process.memoryUsage()
  });
});

console.log('✅ WebSocket server ready');