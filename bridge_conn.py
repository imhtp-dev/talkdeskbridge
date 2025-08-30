#!/usr/bin/env python3
"""
Pipecat Cloud-Talkdesk Bridge Server - Daily.co Integration
Integrates Pipecat Cloud agents with TalkDesk using Daily.co rooms
"""

import redis
import asyncio
import json
import base64
import audioop
import logging
import requests
import signal
import sys
from typing import Optional, Dict, Any, Set, List
from dataclasses import dataclass
from enum import Enum
import uuid
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
import time
from typing import Dict

import aiohttp
import aiomysql

ACTIVE_SESSIONS: Dict[str, "BridgeSession"] = {}

##############################################
# MySQL Configuration (copied from original)
##############################################
DB_CONFIG = {
    "host": "voikdbm74prodzj.mysql.database.azure.com",
    "port": 3306,
    "user": "vjkeitl2sa",
    "password": "2688FV7XGGPedpxx3IyyK",
    "db": "voila_tech_voice",
    "charset": "utf8",
    "autocommit": True,
    "connect_timeout": 2000,
    "ssl": {}
}

async def save_call_to_mysql(call_id: str, assistant_id: str, interaction_id: str, phone_number: str = "") -> bool:
    """MySQL save function (copied from original)"""
    connection = None
    try:
        connection = await aiomysql.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            db=DB_CONFIG["db"],
            charset=DB_CONFIG["charset"],
            autocommit=DB_CONFIG["autocommit"],
            connect_timeout=DB_CONFIG["connect_timeout"],
            ssl=DB_CONFIG["ssl"]
        )
        
        action = "completed"
        
        async with connection.cursor() as cursor:
            query = """
            INSERT INTO tb_stat (assistant_id, interaction_id, call_id, action, phone_number) 
            VALUES (%s, %s, %s, %s, %s)
            """
            
            await cursor.execute(query, (assistant_id, interaction_id, call_id, action, phone_number))
            
            if cursor.rowcount > 0:
                logger.info(f"MySQL: Successfully saved call data - call_id: {call_id}, interaction_id: {interaction_id}, phone_number: {phone_number}")
                return True
            else:
                logger.warning(f"MySQL: No rows affected for call_id: {call_id}")
                return False
                
    except aiomysql.Error as e:
        logger.error(f"MySQL Error saving call {call_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error saving call {call_id}: {e}")
        return False
    finally:
        if connection:
            connection.close()

##############################################
# Redis Configuration (copied from original)
##############################################
PIPECAT_STAT_CONFIG = {
    "base_url": "https://voilavoiceagent-cyf2e9bshnguaebh.westeurope-01.azurewebsites.net",
    "endpoint": "/pipecat_stat",
    "timeout": 30
}

HOST = 'VoilaVoice.redis.cache.windows.net'
PORT = 6380
PASSWORD = '8GhNRK1BsfL5D45MFHoFZpw5j7OkFpZ9BAzCaIp3TFY='
ttl = 24 * 3600

redis_client = redis.StrictRedis(
    host=HOST,
    port=PORT,
    password=PASSWORD,
    ssl=True
)

##############################################
# Pipecat Cloud Configuration
##############################################
PORT = 8080

# Pipecat Cloud settings
PIPECAT_CLOUD_CONFIG = {
    "base_url": "https://api.pipecat.daily.co/v1",  # Base API URL
    "agent_name": "health-booking-assistant",  # Your deployed agent name
    "api_key": os.getenv("PIPECAT_CLOUD_API_KEY"),  # Add your API key to environment
    "timeout": 30
}

# Daily.co configuration
DAILY_CONFIG = {
    "api_key": os.getenv("DAILY_API_KEY"),  # Add your Daily API key to environment
    "base_url": "https://api.daily.co/v1",
    "timeout": 30
}

PIPECAT_ASSISTANT_ID = "12689"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bridge.log')
    ]
)
logger = logging.getLogger('PipecatCloudBridge')

class ConnectionState(Enum):
    INIT = "init"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    CLOSING = "closing"
    CLOSED = "closed"
    ERROR = "error"

class BridgeState(Enum):
    WAITING_START = "waiting_start"
    ACTIVE = "active"
    ESCALATING = "escalating"
    PIPECAT_CLOSED = "pipecat_closed"
    CLOSING = "closing"
    CLOSED = "closed"

@dataclass
class BridgeConfig:
    host: str = "0.0.0.0"
    port: int = PORT
    pipecat_cloud_config: Dict[str, Any] = None
    daily_config: Dict[str, Any] = None
    talkdesk_sample_rate: int = 8000
    pipecat_sample_rate: int = 16000
    channels: int = 1
    chunk_size: int = 160

cfg = BridgeConfig(
    pipecat_cloud_config=PIPECAT_CLOUD_CONFIG,
    daily_config=DAILY_CONFIG
)
app = FastAPI()

class AudioProcessor:
    """Audio processing utilities (copied from original)"""
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
    def resample(audio_data: bytes, from_rate: int, to_rate: int, 
                 channels: int = 1, sample_width: int = 2) -> bytes:
        try:
            if from_rate == to_rate:
                return audio_data
                
            resampled, _ = audioop.ratecv(
                audio_data,
                sample_width,
                channels,
                from_rate,
                to_rate,
                None
            )
            return resampled
        except Exception as e:
            logger.error(f"Error resampling {from_rate}Hz → {to_rate}Hz: {e}")
            return audio_data

class DailyRoomManager:
    """Manages Daily.co room creation and token generation"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.session = aiohttp.ClientSession()
    
    async def create_room(self, room_name: Optional[str] = None) -> Dict[str, Any]:
        """Create a Daily.co room for the Pipecat session"""
        try:
            headers = {
                "Authorization": f"Bearer {self.config['api_key']}",
                "Content-Type": "application/json"
            }
            
            room_config = {
                "privacy": "private",
                "properties": {
                    "start_audio_off": False,
                    "start_video_off": True,
                    "enable_chat": False,
                    "enable_knocking": False,
                    "enable_screenshare": False,
                    "enable_recording": "local",
                    "exp": int(time.time()) + 3600  # 1 hour expiration
                }
            }
            
            if room_name:
                room_config["name"] = room_name
                
            async with self.session.post(
                f"{self.config['base_url']}/rooms",
                headers=headers,
                json=room_config,
                timeout=self.config['timeout']
            ) as response:
                if response.status == 200:
                    room_data = await response.json()
                    logger.info(f"Created Daily room: {room_data['name']}")
                    return room_data
                else:
                    error_text = await response.text()
                    raise Exception(f"Failed to create Daily room: {response.status} - {error_text}")
                    
        except Exception as e:
            logger.error(f"Daily room creation error: {e}")
            raise
    
    async def create_token(self, room_name: str, is_owner: bool = False) -> str:
        """Create a meeting token for the Daily room"""
        try:
            headers = {
                "Authorization": f"Bearer {self.config['api_key']}",
                "Content-Type": "application/json"
            }
            
            token_config = {
                "properties": {
                    "room_name": room_name,
                    "is_owner": is_owner,
                    "exp": int(time.time()) + 3600,  # 1 hour expiration
                    "enable_recording": True
                }
            }
            
            async with self.session.post(
                f"{self.config['base_url']}/meeting-tokens",
                headers=headers,
                json=token_config,
                timeout=self.config['timeout']
            ) as response:
                if response.status == 200:
                    token_data = await response.json()
                    return token_data['token']
                else:
                    error_text = await response.text()
                    raise Exception(f"Failed to create token: {response.status} - {error_text}")
                    
        except Exception as e:
            logger.error(f"Daily token creation error: {e}")
            raise
    
    async def close(self):
        """Close the HTTP session"""
        if self.session:
            await self.session.close()

class PipecatCloudConnection:
    """Manages connection to Pipecat Cloud agent through Daily.co rooms"""
    
    def __init__(self, config: BridgeConfig):
        self.config = config
        self.daily_manager = DailyRoomManager(config.daily_config)
        self.session_id: Optional[str] = None
        self.room_url: Optional[str] = None
        self.room_name: Optional[str] = None
        self.token: Optional[str] = None
        self.state = ConnectionState.INIT
        self.session_data: Dict[str, Any] = {}
        self.http_session = aiohttp.ClientSession()
        
    async def create_pipecat_session(self, business_status: str = "close") -> Dict[str, Any]:
        """Create a new Pipecat Cloud session with Daily.co integration"""
        try:
            self.state = ConnectionState.CONNECTING
            
            # Generate unique session ID
            self.session_id = str(uuid.uuid4())
            
            # Create Daily.co room
            room_name = f"pipecat-session-{self.session_id[:8]}"
            room_data = await self.daily_manager.create_room(room_name)
            self.room_url = room_data["url"]
            self.room_name = room_data["name"]
            
            # Create meeting token
            self.token = await self.daily_manager.create_token(self.room_name, is_owner=False)
            
            # Start Pipecat Cloud session
            await self._start_pipecat_agent_session(business_status)
            
            self.state = ConnectionState.CONNECTED
            
            # Save session data
            self.session_data = {
                'id': self.session_id,
                'room_url': self.room_url,
                'room_name': self.room_name,
                'token': self.token,
                'business_status': business_status,
                'created_at': time.time()
            }
            
            logger.info(f"Pipecat Cloud session created: {self.session_id}")
            logger.info(f"Daily room: {self.room_url}")
            
            return self.session_data
            
        except Exception as e:
            self.state = ConnectionState.ERROR
            logger.error(f"Failed to create Pipecat Cloud session: {e}")
            raise
    
    async def _start_pipecat_agent_session(self, business_status: str):
        """Start the Pipecat Cloud agent session via REST API"""
        try:
            headers = {
                "Authorization": f"Bearer {self.config.pipecat_cloud_config['api_key']}",
                "Content-Type": "application/json"
            }
            
            # Session start payload for Pipecat Cloud
            start_payload = {
                "config": {
                    "room_url": self.room_url,
                    "token": self.token,
                    "business_status": business_status,
                    "session_id": self.session_id
                }
            }
            
            agent_name = self.config.pipecat_cloud_config['agent_name']
            start_url = f"{self.config.pipecat_cloud_config['base_url']}/{agent_name}/start"
            
            async with self.http_session.post(
                start_url,
                headers=headers,
                json=start_payload,
                timeout=self.config.pipecat_cloud_config['timeout']
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    logger.info(f"Pipecat agent session started: {result}")
                else:
                    error_text = await response.text()
                    raise Exception(f"Failed to start Pipecat agent: {response.status} - {error_text}")
                    
        except Exception as e:
            logger.error(f"Pipecat agent session start error: {e}")
            raise
    
    async def close(self):
        """Close the Pipecat Cloud session"""
        self.state = ConnectionState.CLOSING
        try:
            await self.daily_manager.close()
            await self.http_session.close()
            logger.info(f"Pipecat Cloud session closed: {self.session_id}")
        except Exception as e:
            logger.error(f"Error closing Pipecat Cloud session: {e}")
        self.state = ConnectionState.CLOSED

class BridgeSession:
    """Bridge session managing TalkDesk to Pipecat Cloud integration"""
    
    def __init__(self, session_id: str, talkdesk_ws: WebSocket, config: BridgeConfig):
        self.session_id = session_id
        self.talkdesk_ws = talkdesk_ws
        self.config = config
        self.pipecat_conn = PipecatCloudConnection(config)
        self.audio_processor = AudioProcessor()
        self.is_active = False
        self.tasks: Set[asyncio.Task] = set()
        
        self.bridge_state = BridgeState.WAITING_START
        self.escalation_event = asyncio.Event()
        
        self.stream_sid = None
        self.chunk_counter = 0
        self.interaction_id = None
        self.caller_id = None
        self.business_status = None
        
        # Buffer for messages received before Pipecat is ready
        self.audio_buffer = []
        
        self.stats = {
            'talkdesk_to_pipecat_packets': 0,
            'pipecat_to_talkdesk_packets': 0,
            'errors': 0
        }
    
    def extract_business_status(self, business_hours_string: str) -> str:
        """Extract status (open/close) from business_hours string (copied from original)"""
        try:
            if business_hours_string and '::' in business_hours_string:
                parts = business_hours_string.split('::')
                if len(parts) >= 4:
                    status = parts[-1].strip().lower()
                    logger.info(f"Session {self.session_id}: Extracted business status: {status}")
                    return status
            
            logger.warning(f"Session {self.session_id}: Could not extract business status from: {business_hours_string}")
            return "close"
            
        except Exception as e:
            logger.error(f"Session {self.session_id}: Error extracting business status: {e}")
            return "close"
    
    async def initialize_pipecat_with_business_status(self, business_status: str):
        """Initialize Pipecat Cloud session with business status"""
        try:
            logger.info(f"Session {self.session_id}: Initializing Pipecat Cloud with business_status: {business_status}")
            
            # Create Pipecat Cloud session with Daily.co integration
            await self.pipecat_conn.create_pipecat_session(business_status)
            
            # Change state to ACTIVE
            self.set_bridge_state(BridgeState.ACTIVE)
            
            logger.info(f"Session {self.session_id}: Pipecat Cloud initialized successfully with business_status: {business_status}")
            
            # Note: Audio forwarding will be handled differently with Daily.co
            # We don't send buffered audio directly - Daily.co handles the media streaming
            
            return True
            
        except Exception as e:
            logger.error(f"Session {self.session_id}: Failed to initialize Pipecat Cloud: {e}")
            self.set_bridge_state(BridgeState.ERROR)
            return False
    
    def set_bridge_state(self, new_state: BridgeState):
        """Set bridge state with logging (copied from original)"""
        old_state = self.bridge_state
        self.bridge_state = new_state
        logger.info(f"Session {self.session_id}: Bridge state changed {old_state.value} → {new_state.value}")
    
    async def start_escalation(self) -> bool:
        """Start escalation process (adapted for Daily.co)"""
        try:
            if self.bridge_state != BridgeState.ACTIVE:
                logger.warning(f"Session {self.session_id}: Cannot start escalation, state is {self.bridge_state.value}")
                return False
            
            logger.info(f"Session {self.session_id}: Starting escalation process")
            self.set_bridge_state(BridgeState.ESCALATING)
            
            await self.pipecat_conn.close()
            logger.info(f"Session {self.session_id}: Pipecat Cloud session closed for escalation")
            
            self.escalation_event.set()
            await asyncio.sleep(2)
            
            self.set_bridge_state(BridgeState.PIPECAT_CLOSED)
            logger.info(f"Session {self.session_id}: Escalation ready - Pipecat session completed")
            
            return True
            
        except Exception as e:
            logger.error(f"Session {self.session_id}: Error during escalation start: {e}")
            return False
    
    async def complete_escalation(self, stop_msg: Dict[str, Any]) -> bool:
        """Complete escalation process (copied from original)"""
        try:
            if self.bridge_state not in [BridgeState.ESCALATING, BridgeState.PIPECAT_CLOSED]:
                logger.warning(f"Session {self.session_id}: Cannot complete escalation, state is {self.bridge_state.value}")
                return False
            
            logger.info(f"Session {self.session_id}: Completing escalation")
            
            await self.talkdesk_ws.send_text(json.dumps(stop_msg))
            logger.info(f"Session {self.session_id}: Escalation message sent to Talkdesk")
            
            self.set_bridge_state(BridgeState.CLOSING)
            
            return True
            
        except Exception as e:
            logger.error(f"Session {self.session_id}: Error completing escalation: {e}")
            return False
    
    async def start(self):
        """Start bridge session with Daily.co integration"""
        try:
            logger.info(f"Starting bridge session: {self.session_id}")
            
            self.is_active = True
            self.set_bridge_state(BridgeState.WAITING_START)
            
            # Start TalkDesk message processing
            forward_task = asyncio.create_task(self._process_talkdesk_messages())
            self.tasks = {forward_task}
            
            while self.is_active and self.bridge_state not in [BridgeState.CLOSING, BridgeState.CLOSED]:
                done, pending = await asyncio.wait(
                    self.tasks, 
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=1.0
                )
                
                if self.bridge_state in [BridgeState.ESCALATING, BridgeState.PIPECAT_CLOSED]:
                    for task in done:
                        if task in self.tasks:
                            self.tasks.remove(task)
                    continue
                
                if done and self.bridge_state == BridgeState.ACTIVE:
                    logger.info(f"Session {self.session_id}: Normal termination - task completed")
                    break
                    
            for task in self.tasks:
                if not task.done():
                    task.cancel()
                    
        except Exception as e:
            logger.error(f"Session {self.session_id} error: {e}")
            self.stats['errors'] += 1
        finally:
            await self.stop()
    
    async def _process_talkdesk_messages(self):
        """Process TalkDesk messages with Daily.co integration"""
        logger.info(f"Session {self.session_id}: Starting TalkDesk message processing")
        
        try:
            while self.is_active:
                if self.bridge_state in [BridgeState.PIPECAT_CLOSED, BridgeState.ESCALATING]:
                    await asyncio.sleep(0.5)
                    continue
                
                message = await self.talkdesk_ws.receive_text()
                
                try:
                    data = json.loads(message)
                    event = data.get('event')
                    
                    if event == 'start':
                        logger.info(f"Session {self.session_id}: Received START from TalkDesk")
                        
                        # Extract session data
                        self.stream_sid = data.get('streamSid')
                        if not self.stream_sid and 'start' in data:
                            self.stream_sid = data['start'].get('streamSid')
                        
                        if 'start' in data:
                            self.interaction_id = data['start'].get('customParameters', {}).get('interaction_id')
                            
                            # Extract business_hours and determine status
                            custom_params = data['start'].get('customParameters', {})
                            business_hours = custom_params.get('business_hours', '')
                            self.caller_id = custom_params.get('caller_id', '')
                            
                            logger.info(f"[{self.session_id}] Raw business_hours: {business_hours}")
                            logger.info(f"[{self.session_id}] Caller ID: {self.caller_id}")
                            
                            # Extract status (open/close)
                            self.business_status = self.extract_business_status(business_hours)
                            logger.info(f"[{self.session_id}] Extracted business status: {self.business_status}")
                            
                            # Initialize Pipecat Cloud session
                            pipecat_initialized = await self.initialize_pipecat_with_business_status(self.business_status)
                            
                            if pipecat_initialized:
                                logger.info(f"✅ [{self.session_id}] Pipecat Cloud initialized with status: {self.business_status}")
                            else:
                                logger.error(f"❌ [{self.session_id}] Failed to initialize Pipecat Cloud")
                                break
                        
                        logger.info(f"[{self.session_id}] streamSid: {self.stream_sid} | "
                                   f"Pipecat Session ID: {self.pipecat_conn.session_id} | "
                                   f"Interaction ID: {self.interaction_id} | "
                                   f"Business Status: {self.business_status} | "
                                   f"Caller ID: {self.caller_id}")
                        
                        if self.stream_sid:
                            ACTIVE_SESSIONS[self.stream_sid] = self
                        
                        # Save to Redis with caller_id
                        if self.pipecat_conn.session_id:
                            redis_client.hset(
                                self.pipecat_conn.session_id, 
                                mapping={
                                    "interaction_id": self.interaction_id, 
                                    "stream_sid": self.stream_sid,
                                    "caller_id": self.caller_id,
                                    "room_url": self.pipecat_conn.room_url
                                }
                            )
                        
                        logger.info(f"Session {self.session_id}: START processing completed with caller_id: {self.caller_id}")
                        continue
                        
                    elif event == 'stop':
                        logger.info(f"🛑 Session {self.session_id}: Received STOP from TalkDesk (patient hung up)")
                        
                        # MySQL save logic with caller_id (copied from original)
                        try:
                            call_id = self.pipecat_conn.session_id
                            assistant_id = PIPECAT_ASSISTANT_ID
                            
                            if not self.interaction_id:
                                interaction_id = redis_client.hget(call_id, "interaction_id")
                                if isinstance(interaction_id, bytes):
                                    interaction_id = interaction_id.decode()
                                self.interaction_id = interaction_id
                            
                            # Retrieve caller_id from Redis if not available locally
                            if not self.caller_id:
                                caller_id = redis_client.hget(call_id, "caller_id")
                                if isinstance(caller_id, bytes):
                                    caller_id = caller_id.decode()
                                self.caller_id = caller_id or ""
                            
                            logger.info(f"💾 Saving to MySQL: call_id={call_id}, "
                                       f"assistant_id={assistant_id}, "
                                       f"interaction_id={self.interaction_id}, "
                                       f"phone_number={self.caller_id}")
                            
                            mysql_success = await save_call_to_mysql(
                                call_id=call_id,
                                assistant_id=assistant_id,
                                interaction_id=self.interaction_id,
                                phone_number=self.caller_id
                            )
                            
                            if mysql_success:
                                logger.info(f"✅ MySQL save successful for call {call_id} with phone_number {self.caller_id}")
                            else:
                                logger.error(f"❌ MySQL save failed for call {call_id}")
                                
                        except Exception as mysql_error:
                            logger.error(f"❌ MySQL save error: {str(mysql_error)}")
                        
                        break
                        
                    elif event == 'media':
                        # Media handling - with Daily.co, we don't forward audio directly
                        # Daily.co handles the audio streaming between TalkDesk and Pipecat
                        # We can log media events for monitoring
                        if self.bridge_state == BridgeState.ACTIVE:
                            media = data.get('media', {})
                            if media.get('track') == 'inbound':
                                self.stats['talkdesk_to_pipecat_packets'] += 1
                                
                                # With Daily.co integration, audio is handled through the Daily room
                                # No need to manually forward audio packets
                                logger.debug(f"Session {self.session_id}: Media packet received (handled by Daily.co)")
                        
                except json.JSONDecodeError:
                    logger.error(f"Session {self.session_id}: Invalid JSON from TalkDesk")
                except Exception as e:
                    logger.error(f"Session {self.session_id}: Error processing TalkDesk message: {e}")
                    self.stats['errors'] += 1
                    
        except Exception as e:
            logger.error(f"Session {self.session_id}: Message processing error: {e}")
            self.stats['errors'] += 1
    
    async def stop(self):
        """Stop bridge session (adapted for Daily.co)"""
        logger.info(f"Stopping session {self.session_id}")
        self.is_active = False
        self.set_bridge_state(BridgeState.CLOSED)
        
        logger.info(f"Session {self.session_id} stats: "
                   f"TalkDesk→Pipecat: {self.stats['talkdesk_to_pipecat_packets']}, "
                   f"Pipecat→TalkDesk: {self.stats['pipecat_to_talkdesk_packets']}, "
                   f"Errors: {self.stats['errors']}")
        
        if self.pipecat_conn.state not in [ConnectionState.CLOSED, ConnectionState.CLOSING]:
            await self.pipecat_conn.close()
        
        if self.bridge_state != BridgeState.PIPECAT_CLOSED:
            try:
                await self.talkdesk_ws.send_text(json.dumps({"event": "stop"}))
            except Exception:
                pass

##############################################
# FastAPI Endpoints
##############################################

@app.get("/healthz")
async def healthz():
    return {"status": "ok", "service": "pipecat-cloud-bridge"}

@app.websocket("/talkdesk")
async def talkdesk_ws(ws: WebSocket):
    await ws.accept()
    session_id = str(uuid.uuid4())
    logger.info(f"New TalkDesk connection – Session: {session_id}")
    session = BridgeSession(session_id, ws, cfg)
    ACTIVE_SESSIONS[session_id] = session

    try:
        await session.start()
    except WebSocketDisconnect:
        logger.info(f"Session {session_id} disconnected")
    finally:
        ACTIVE_SESSIONS.pop(session_id, None)
        logger.info(f"Session {session_id} ended")

##############################################
# Pipecat Cloud Stats Function
##############################################

async def call_pipecat_stat_internal(call_id: str, interaction_id: str) -> Optional[Dict[str, Any]]:
    """Call Pipecat Cloud statistics service (placeholder implementation)"""
    try:
        # For now return default data - you can implement actual Pipecat Cloud API call
        return {
            "success": True,
            "action": "transfer",
            "sentiment": "neutral",
            "duration_seconds": 0,
            "cost": 0,
            "summary": "Call handled by Pipecat Cloud",
            "service": "5"
        }
        
    except Exception as e:
        logger.error(f"Error calling pipecat_stat: {str(e)}")
        return None

##############################################
# Helper Functions (copied from original)
##############################################

def limita_testo_256(text):
    """Limit text to 256 characters (copied from original)"""
    max_length = 240
    if len(text) <= max_length:
        return text
    truncated = text[:max_length]
    last_space = truncated.rfind(" ")
    if last_space != -1:
        truncated = truncated[:last_space]
    return truncated.strip() + ""

def build_talkdesk_message(stream_sid: str, pipecat_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Build TalkDesk escalation message (copied from original)"""
    action = "transfer"
    sentiment = "neutral"
    duration = "0"
    cost = "0"
    summary = "richiesta di assistenza"
    service = "2|2|5"
    
    if pipecat_data:
        action = pipecat_data.get("action", "transfer")
        sentiment = pipecat_data.get("sentiment", "neutral")
        duration = str(int(pipecat_data.get("duration_seconds", 0)))
        cost = str(pipecat_data.get("cost", 0))
        summary = pipecat_data.get("summary", "richiesta di assistenza")
        servicex = str(pipecat_data.get("service", "5"))
        if servicex is None or str(servicex).strip() == "":
            servicex = "5"
        else:
            servicex = str(servicex).strip()
        service = f"2|2|{servicex}"
    
    summary = limita_testo_256(summary)
    ring_group = f"{summary}::{sentiment}::{action}::{duration}::{service}"
    
    stop_msg = {
        "event": "stop",
        "streamSid": stream_sid,
        "stop": {
            "command": "escalate",
            "ringGroup": ring_group
        }
    }
    return stop_msg

##############################################
# Escalation Endpoint (copied from original)
##############################################

@app.post("/escalation")
async def escalation(request: Request) -> Dict[str, Any]:
    """Endpoint for handling escalation (compatible with existing system)"""
    payload = await request.json()
    call_id = payload.get("message", {}).get("call", {}).get("id")
    tool_calls = payload.get("message", {}).get("toolCallList", []) or [{}]
    
    results = [{
        "toolCallId": tc.get("id"),
        "result": call_id or "Error: call_id not found"
    } for tc in tool_calls]
    
    if call_id:
        interaction_id = redis_client.hget(call_id, "interaction_id")
        stream_sid = redis_client.hget(call_id, "stream_sid")
        
        if isinstance(stream_sid, bytes):
            stream_sid = stream_sid.decode()
        if isinstance(interaction_id, bytes):
            interaction_id = interaction_id.decode()
            
        if stream_sid:
            session = ACTIVE_SESSIONS.get(stream_sid)
            if session:
                try:
                    await asyncio.sleep(1.5)
                    logger.info(f"🔄 Starting immediate Pipecat Cloud closure for call {call_id}")
                    
                    escalation_started = await session.start_escalation()
                    
                    if not escalation_started:
                        raise Exception("Failed to start escalation process")
                    
                    logger.info(f"✅ Pipecat Cloud session closed immediately for call {call_id}")
                    
                    logger.info(f"⏳ Waiting for Pipecat Cloud to complete...")
                    await asyncio.sleep(2)
                    
                    logger.info(f"📊 Fetching final call data...")
                    
                    pipecat_data = await call_pipecat_stat_internal(call_id, interaction_id)
                    
                    if pipecat_data:
                        logger.info(f"✅ Got Pipecat data successfully")
                    else:
                        logger.warning(f"⚠️ No Pipecat data received, using defaults")
                    
                    stop_msg = build_talkdesk_message(stream_sid, pipecat_data)
                    
                    escalation_completed = await session.complete_escalation(stop_msg)
                    
                    if escalation_completed:
                        logger.info(f"📞 Escalation completed successfully: {stop_msg}")
                        logger.info(f"✅ Escalation process finished for {stream_sid}")
                    else:
                        raise Exception("Failed to complete escalation process")
                    
                except Exception as e:
                    logger.error(f"❌ Error during escalation: {str(e)}")
                    
                    try:
                        logger.info(f"🔄 Attempting fallback escalation with default data...")
                        stop_msg = build_talkdesk_message(stream_sid, None)
                        
                        if session.bridge_state not in [BridgeState.CLOSED]:
                            await session.talkdesk_ws.send_text(json.dumps(stop_msg))
                            logger.info(f"✅ Fallback escalation sent")
                        else:
                            logger.error(f"❌ Session already closed, cannot send fallback")
                            
                    except Exception as fallback_error:
                        logger.error(f"❌ Fallback escalation also failed: {str(fallback_error)}")
            else:
                logger.warning(f"Escalation: no active session for streamSid {stream_sid}")
        else:
            logger.warning(f"Escalation: streamSid not found for call_id {call_id}")
    else:
        logger.warning("Escalation: call_id missing")
    
    return {"results": results}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)