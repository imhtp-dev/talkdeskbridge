#!/usr/bin/env python3
"""
TalkDesk-Pipecat WebSocket Bridge
Connects TalkDesk to Pipecat WebSocket server for audio streaming
"""
import asyncio
import json
import base64
import audioop
import logging
import uuid
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Dict, Set
import time
from pipecat.transports.network.websocket_client import WebsocketClientTransport, WebsocketClientParams
from pipecat.serializers.protobuf import ProtobufFrameSerializer
from pipecat.frames.frames import InputAudioRawFrame, OutputAudioRawFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask, PipelineParams
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection

##############################################
# Configuration
##############################################
PORT = 8080
PIPECAT_SERVER_URL = os.getenv("PIPECAT_SERVER_URL", "http://localhost:8765")  # Pipecat WebSocket server URL

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('PipecatBridge')

app = FastAPI()
ACTIVE_SESSIONS: Dict[str, "BridgeSession"] = {}

##############################################
# Audio Processing
##############################################
class AudioProcessor:
    @staticmethod
    def mulaw_to_pcm(mulaw_data: bytes) -> bytes:
        try:
            return audioop.ulaw2lin(mulaw_data, 2)
        except Exception as e:
            logger.error(f"Error converting μ-law → PCM: {e}")
            return b''
    
    @staticmethod
    def pcm_to_mulaw(pcm_data: bytes) -> bytes:
        try:
            return audioop.lin2ulaw(pcm_data, 2)
        except Exception as e:
            logger.error(f"Error converting PCM → μ-law: {e}")
            return b''
    
    @staticmethod
    def resample(audio_data: bytes, from_rate: int, to_rate: int) -> bytes:
        try:
            if from_rate == to_rate:
                return audio_data
            resampled, _ = audioop.ratecv(audio_data, 2, 1, from_rate, to_rate, None)
            return resampled
        except Exception as e:
            logger.error(f"Error resampling {from_rate}Hz → {to_rate}Hz: {e}")
            return audio_data

##############################################
# Audio Frame Processors
##############################################
class TalkDeskAudioInputProcessor(FrameProcessor):
    """Processes audio from TalkDesk and converts to Pipecat format"""
    
    def __init__(self, bridge_session, **kwargs):
        super().__init__(**kwargs)
        self.bridge_session = bridge_session
        self.audio_processor = AudioProcessor()
    
    async def process_frame(self, frame, direction: FrameDirection):
        # This processor mainly handles TalkDesk → Pipecat audio conversion
        # The actual audio input comes from TalkDesk WebSocket, not frames
        await super().process_frame(frame, direction)
        await self.push_frame(frame, direction)
    
    async def process_talkdesk_audio(self, mulaw_data: bytes):
        """Convert TalkDesk audio to Pipecat format and push to pipeline"""
        try:
            # Convert μ-law 8kHz to PCM 16kHz for Pipecat
            pcm_8khz = self.audio_processor.mulaw_to_pcm(mulaw_data)
            pcm_16khz = self.audio_processor.resample(pcm_8khz, 8000, 16000)
            
            # Create InputAudioRawFrame for Pipecat
            audio_frame = InputAudioRawFrame(
                audio=pcm_16khz,
                sample_rate=16000,
                num_channels=1
            )
            
            # Push to pipeline
            await self.push_frame(audio_frame, FrameDirection.DOWNSTREAM)
            
        except Exception as e:
            logger.error(f"Error processing TalkDesk audio: {e}")

class TalkDeskAudioOutputProcessor(FrameProcessor):
    """Processes audio from Pipecat and sends to TalkDesk"""
    
    def __init__(self, bridge_session, **kwargs):
        super().__init__(**kwargs)
        self.bridge_session = bridge_session
        self.audio_processor = AudioProcessor()
    
    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        
        if isinstance(frame, OutputAudioRawFrame):
            await self.send_to_talkdesk(frame.audio)
        
        await self.push_frame(frame, direction)
    
    async def send_to_talkdesk(self, pcm_16khz: bytes):
        """Convert Pipecat audio to TalkDesk format and send"""
        try:
            # Convert PCM 16kHz to μ-law 8kHz for TalkDesk
            pcm_8khz = self.audio_processor.resample(pcm_16khz, 16000, 8000)
            mulaw_data = self.audio_processor.pcm_to_mulaw(pcm_8khz)
            payload = base64.b64encode(mulaw_data).decode()
            
            self.bridge_session.chunk_counter += 1
            
            # Send to TalkDesk
            message = {
                "event": "media",
                "streamSid": self.bridge_session.stream_sid,
                "media": {
                    "track": "outbound",
                    "chunk": str(self.bridge_session.chunk_counter),
                    "timestamp": str(int(time.time() * 1000)),
                    "payload": payload
                }
            }
            
            await self.bridge_session.talkdesk_ws.send_text(json.dumps(message))
            
        except Exception as e:
            logger.error(f"Error sending audio to TalkDesk: {e}")

##############################################
# Pipecat Bridge Session
##############################################
class BridgeSession:
    def __init__(self, session_id: str, talkdesk_ws: WebSocket):
        self.session_id = session_id
        self.talkdesk_ws = talkdesk_ws
        self.pipecat_transport = None
        self.is_active = False
        self.tasks: Set[asyncio.Task] = set()
        self.stream_sid = None
        self.chunk_counter = 0
        self.pipeline_task = None
        self.pipeline_runner = None
        
        # Audio processors
        self.input_processor = TalkDeskAudioInputProcessor(self)
        self.output_processor = TalkDeskAudioOutputProcessor(self)
        
    async def start(self):
        """Start bridge session with Pipecat WebSocket client"""
        try:
            logger.info(f"Starting Pipecat bridge session: {self.session_id}")
            self.is_active = True
            
            # Create Pipecat WebSocket client transport
            self.pipecat_transport = WebsocketClientTransport(
                uri=PIPECAT_SERVER_URL,
                params=WebsocketClientParams(
                    audio_in_enabled=True,
                    audio_out_enabled=True,
                    add_wav_header=False,
                    serializer=ProtobufFrameSerializer()
                )
            )
            
            # Create pipeline with audio processors
            pipeline = Pipeline([
                self.pipecat_transport.input(),
                self.input_processor,
                self.output_processor,
                self.pipecat_transport.output()
            ])
            
            # Create pipeline task
            self.pipeline_task = PipelineTask(
                pipeline,
                params=PipelineParams(
                    allow_interruptions=True,
                    enable_metrics=False
                )
            )
            
            # Setup event handlers
            self._setup_event_handlers()
            
            # Start TalkDesk WebSocket handler
            talkdesk_task = asyncio.create_task(self._handle_talkdesk_messages())
            self.tasks.add(talkdesk_task)
            
            # Start Pipecat pipeline
            self.pipeline_runner = PipelineRunner()
            pipeline_task = asyncio.create_task(self.pipeline_runner.run(self.pipeline_task))
            self.tasks.add(pipeline_task)
            
            # Wait for tasks to complete
            await asyncio.gather(*self.tasks, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Session {self.session_id} error: {e}")
        finally:
            await self.stop()
    
    def _setup_event_handlers(self):
        """Setup Pipecat transport event handlers"""
        
        @self.pipecat_transport.event_handler("on_connected")
        async def on_pipecat_connected(transport):
            logger.success(f"Session {self.session_id}: Connected to Pipecat server")
        
        @self.pipecat_transport.event_handler("on_disconnected")
        async def on_pipecat_disconnected(transport):
            logger.info(f"Session {self.session_id}: Disconnected from Pipecat server")
            await self.stop()
        
        @self.pipecat_transport.event_handler("on_connection_error")
        async def on_pipecat_error(transport, error):
            logger.error(f"Session {self.session_id}: Pipecat connection error: {error}")
            await self.stop()
    
    async def _handle_talkdesk_messages(self):
        """Handle incoming messages from TalkDesk WebSocket"""
        logger.info(f"Session {self.session_id}: Starting TalkDesk message handler")
        
        try:
            while self.is_active:
                message = await self.talkdesk_ws.receive_text()
                data = json.loads(message)
                event = data.get('event')
                
                if event == 'start':
                    logger.info(f"Session {self.session_id}: Received START from TalkDesk")
                    self.stream_sid = data.get('streamSid', data.get('start', {}).get('streamSid'))
                    if self.stream_sid:
                        ACTIVE_SESSIONS[self.stream_sid] = self
                    
                elif event == 'stop':
                    logger.info(f"Session {self.session_id}: Received STOP from TalkDesk")
                    break
                    
                elif event == 'media':
                    # Process incoming audio from TalkDesk
                    media = data.get('media', {})
                    if media.get('track') == 'inbound':
                        payload = media.get('payload', '')
                        
                        # Decode and process audio
                        mulaw_data = base64.b64decode(payload)
                        await self.input_processor.process_talkdesk_audio(mulaw_data)
                        
        except Exception as e:
            logger.error(f"Session {self.session_id}: TalkDesk message handler error: {e}")
    
    async def stop(self):
        """Stop bridge session"""
        logger.info(f"Stopping Pipecat bridge session {self.session_id}")
        self.is_active = False
        
        # Cancel tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
        
        # Stop pipeline
        if self.pipeline_task:
            await self.pipeline_task.cancel()
        
        # Close transport
        if self.pipecat_transport:
            await self.pipecat_transport.close()

##############################################
# FastAPI Endpoints
##############################################
@app.get("/healthz")
async def healthz():
    return {"status": "ok", "service": "talkdesk-pipecat-bridge"}

@app.websocket("/talkdesk")
async def talkdesk_ws(ws: WebSocket):
    await ws.accept()
    session_id = str(uuid.uuid4())
    logger.info(f"New TalkDesk connection – Session: {session_id}")
    
    session = BridgeSession(session_id, ws)
    ACTIVE_SESSIONS[session_id] = session

    try:
        await session.start()
    except WebSocketDisconnect:
        logger.info(f"Session {session_id} disconnected")
    finally:
        ACTIVE_SESSIONS.pop(session_id, None)
        ACTIVE_SESSIONS.pop(session.stream_sid, None)
        logger.info(f"Session {session_id} ended")

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting TalkDesk-Pipecat Bridge on port {PORT}")
    logger.info(f"Pipecat Server URL: {PIPECAT_SERVER_URL}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)