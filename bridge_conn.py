#!/usr/bin/env python3
"""
Pipecat-Talkdesk Bridge Server - Twilio WebSocket Transport Integration
Direct forwarding approach using Pipecat Cloud's Twilio WebSocket endpoint
"""
import redis
import asyncio
import websockets
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
import aiohttp
import aiomysql

ACTIVE_SESSIONS: Dict[str, "BridgeSession"] = {}

##############################################
# Pipecat Cloud Configuration
##############################################
PIPECAT_ORGANIZATION = os.getenv("PIPECAT_ORGANIZATION", "imhtp-workspace")  # Your org name from 'pcc organizations list'
PIPECAT_AGENT_NAME = os.getenv("PIPECAT_AGENT_NAME", "health-booking-assistant")  # Your agent name

if not PIPECAT_ORGANIZATION:
    logging.error("PIPECAT_ORGANIZATION environment variable required!")
    logging.error("Get it with: pcc organizations list")
    sys.exit(1)

# Pipecat Cloud Twilio WebSocket URL
PIPECAT_WS_BASE_URL = "wss://api.pipecat.daily.co/ws/twilio"

##############################################
# MySQL Configuration (unchanged)
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
    connection = None
    try:
        connection = await aiomysql.connect(**DB_CONFIG)
        action = "completed"
        
        async with connection.cursor() as cursor:
            query = """
            INSERT INTO tb_stat (assistant_id, interaction_id, call_id, action, phone_number) 
            VALUES (%s, %s, %s, %s, %s)
            """
            await cursor.execute(query, (assistant_id, interaction_id, call_id, action, phone_number))
            
            if cursor.rowcount > 0:
                logger.info(f"MySQL: Successfully saved call data - call_id: {call_id}")
                return True
            else:
                logger.warning(f"MySQL: No rows affected for call_id: {call_id}")
                return False
                
    except Exception as e:
        logger.error(f"MySQL Error saving call {call_id}: {e}")
        return False
    finally:
        if connection:
            connection.close()

##############################################
# Redis Configuration (unchanged)
##############################################
HOST = 'VoilaVoice.redis.cache.windows.net'
PORT = 6380
PASSWORD = '8GhNRK1BsfL5D45MFHoFZpw5j7OkFpZ9BAzCaIp3TFY='

redis_client = redis.StrictRedis(
    host=HOST,
    port=PORT,
    password=PASSWORD,
    ssl=True
)

##############################################
# Server Configuration
##############################################
PORT = 8000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bridge.log')
    ]
)
logger = logging.getLogger('PipecatTwilioBridge')

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

class PipecatTwilioConnection:
    """Manages connection to Pipecat Cloud via Twilio WebSocket protocol"""
    
    def __init__(self):
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.call_id: Optional[str] = None
        self.state = ConnectionState.INIT
        self.sequence_number = 0
        
    def get_next_sequence_number(self) -> str:
        """Get next sequence number for Twilio protocol"""
        self.sequence_number += 1
        return str(self.sequence_number)
        
    async def create_connection(self, business_status: str = "close") -> Dict[str, Any]:
        """Create Pipecat Cloud WebSocket URL with service host parameter"""
        try:
            self.state = ConnectionState.CONNECTING
            self.call_id = str(uuid.uuid4())
            
            # Build Pipecat Cloud Twilio WebSocket URL with service host parameter
            service_host = f"{PIPECAT_AGENT_NAME}.{PIPECAT_ORGANIZATION}"
            pipecat_ws_url = f"{PIPECAT_WS_BASE_URL}?_pipecatCloudServiceHost={service_host}"
            
            logger.info(f"Creating Pipecat Twilio WebSocket connection")
            logger.info(f"Service Host: {service_host}")
            logger.info(f"WebSocket URL: {pipecat_ws_url}")
            
            session_data = {
                'id': self.call_id,
                'business_status': business_status,
                'service_host': service_host,
                'ws_url': pipecat_ws_url,
                'created_at': time.time()
            }
            
            return session_data
            
        except Exception as e:
            self.state = ConnectionState.ERROR
            logger.error(f"Failed to create Pipecat connection: {e}")
            raise
    
    async def connect(self, ws_url: str):
        """Connect to Pipecat Cloud Twilio WebSocket"""
        try:
            logger.info(f"Connecting to Pipecat Twilio WebSocket: {ws_url}")
            
            self.websocket = await websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            )
            
            self.state = ConnectionState.CONNECTED
            logger.info(f"Connected to Pipecat Twilio WebSocket: {self.call_id}")
            
            # Send initial connected message (Twilio protocol)
            connected_msg = {
                "event": "connected",
                "protocol": "Call",
                "version": "1.0.0"
            }
            await self.websocket.send(json.dumps(connected_msg))
            logger.info("Sent connected message to Pipecat")
            
        except Exception as e:
            self.state = ConnectionState.ERROR
            logger.error(f"Failed to connect to Pipecat Twilio WebSocket: {e}")
            raise
    
    async def send_twilio_message(self, message: Dict[str, Any]):
        """Send Twilio-formatted message to Pipecat"""
        if self.websocket and self.state == ConnectionState.CONNECTED:
            try:
                # Add sequence number if not present
                if "sequenceNumber" not in message and message.get("event") in ["start", "media", "stop"]:
                    message["sequenceNumber"] = self.get_next_sequence_number()
                
                await self.websocket.send(json.dumps(message))
                logger.debug(f"Sent to Pipecat: {message.get('event', 'unknown')}")
                
            except Exception as e:
                logger.error(f"Error sending message to Pipecat: {e}")
                self.state = ConnectionState.ERROR
                raise
    
    async def receive_twilio_message(self) -> Optional[Dict[str, Any]]:
        """Receive Twilio-formatted message from Pipecat"""
        if self.websocket and self.state == ConnectionState.CONNECTED:
            try:
                data = await self.websocket.recv()
                if isinstance(data, str):
                    message = json.loads(data)
                    logger.debug(f"Received from Pipecat: {message.get('event', 'unknown')}")
                    return message
                else:
                    # Handle binary data if needed
                    return None
            except websockets.exceptions.ConnectionClosed:
                logger.info("Pipecat WebSocket connection closed")
                self.state = ConnectionState.CLOSED
                return None
            except json.JSONDecodeError:
                logger.error("Invalid JSON from Pipecat")
                return None
            except Exception as e:
                logger.error(f"Error receiving from Pipecat: {e}")
                raise
        return None
    
    async def close(self):
        """Close Pipecat connection"""
        self.state = ConnectionState.CLOSING
        if self.websocket:
            try:
                await self.websocket.close()
                logger.info(f"Pipecat Twilio WebSocket closed: {self.call_id}")
            except Exception as e:
                logger.error(f"Error closing Pipecat connection: {e}")
        self.state = ConnectionState.CLOSED

class BridgeSession:
    def __init__(self, session_id: str, talkdesk_ws: WebSocket):
        self.session_id = session_id
        self.talkdesk_ws = talkdesk_ws
        self.pipecat_conn = PipecatTwilioConnection()
        self.is_active = False
        self.tasks: Set[asyncio.Task] = set()
        
        self.bridge_state = BridgeState.WAITING_START
        self.escalation_event = asyncio.Event()
        
        self.stream_sid = None
        self.interaction_id = None
        self.caller_id = None
        self.business_status = None
        self.pipecat_ws_url = None
        
        # Message buffer for before Pipecat is ready
        self.message_buffer = []
        
        self.stats = {
            'talkdesk_to_pipecat_messages': 0,
            'pipecat_to_talkdesk_messages': 0,
            'errors': 0
        }
    
    def extract_business_status(self, business_hours_string: str) -> str:
        """Extract status from business_hours string"""
        try:
            if business_hours_string and '::' in business_hours_string:
                parts = business_hours_string.split('::')
                if len(parts) >= 4:
                    status = parts[-1].strip().lower()
                    logger.info(f"Session {self.session_id}: Extracted business status: {status}")
                    return status
            
            logger.warning(f"Session {self.session_id}: Could not extract business status")
            return "close"
            
        except Exception as e:
            logger.error(f"Session {self.session_id}: Error extracting business status: {e}")
            return "close"
    
    def talkdesk_to_twilio_format(self, talkdesk_msg: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Talkdesk message format to Twilio format"""
        event = talkdesk_msg.get('event')
        
        if event == 'start':
            # Convert Talkdesk start to Twilio start
            twilio_msg = {
                "event": "start",
                "start": {
                    "accountSid": f"AC{self.session_id[:30]}",  # Fake account SID
                    "callSid": f"CA{self.pipecat_conn.call_id[:30]}",  # Fake call SID  
                    "streamSid": talkdesk_msg.get('streamSid', ''),
                    "tracks": ["inbound", "outbound"],
                    "mediaFormat": {
                        "encoding": "audio/x-mulaw",
                        "sampleRate": 8000,
                        "channels": 1
                    }
                }
            }
            
            # Add custom parameters from Talkdesk
            if 'start' in talkdesk_msg and 'customParameters' in talkdesk_msg['start']:
                custom_params = talkdesk_msg['start']['customParameters']
                # Add business_status to custom parameters
                custom_params['business_status'] = self.business_status
                twilio_msg['start']['customParameters'] = custom_params
            
            return twilio_msg
            
        elif event == 'media':
            # Media messages are already very similar
            twilio_msg = {
                "event": "media",
                "streamSid": talkdesk_msg.get('streamSid', ''),
                "media": talkdesk_msg.get('media', {})
            }
            return twilio_msg
            
        elif event == 'stop':
            # Stop messages are simple
            twilio_msg = {
                "event": "stop", 
                "streamSid": talkdesk_msg.get('streamSid', '')
            }
            return twilio_msg
        
        else:
            # Pass through other messages as-is
            return talkdesk_msg
    
    def twilio_to_talkdesk_format(self, twilio_msg: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Twilio message format back to Talkdesk format"""
        event = twilio_msg.get('event')
        
        if event == 'media':
            # Convert Twilio media back to Talkdesk format
            talkdesk_msg = {
                "event": "media",
                "streamSid": self.stream_sid,  # Use original Talkdesk stream SID
                "media": twilio_msg.get('media', {})
            }
            
            # Ensure we have the required fields for Talkdesk
            if 'media' in talkdesk_msg:
                media = talkdesk_msg['media']
                if 'chunk' not in media:
                    media['chunk'] = str(int(time.time()))
                if 'timestamp' not in media:
                    media['timestamp'] = str(int(time.time() * 1000))
                    
            return talkdesk_msg
        
        else:
            # For other events, pass through or ignore
            return twilio_msg
    
    async def initialize_pipecat_with_business_status(self, business_status: str):
        """Initialize Pipecat Twilio WebSocket with business_status"""
        try:
            logger.info(f"Session {self.session_id}: Initializing Pipecat Twilio WebSocket")
            logger.info(f"Business Status: {business_status}")
            
            # Create Pipecat connection
            session_data = await self.pipecat_conn.create_connection(business_status)
            self.pipecat_ws_url = session_data['ws_url']
            
            # Connect to Pipecat Twilio WebSocket
            await self.pipecat_conn.connect(self.pipecat_ws_url)
            
            # Change state to ACTIVE
            self.set_bridge_state(BridgeState.ACTIVE)
            
            logger.info(f"Session {self.session_id}: Pipecat Twilio WebSocket initialized successfully")
            
            # Send buffered messages
            if self.message_buffer:
                logger.info(f"Session {self.session_id}: Sending {len(self.message_buffer)} buffered messages")
                for msg in self.message_buffer:
                    try:
                        twilio_msg = self.talkdesk_to_twilio_format(msg)
                        await self.pipecat_conn.send_twilio_message(twilio_msg)
                        self.stats['talkdesk_to_pipecat_messages'] += 1
                    except Exception as e:
                        logger.error(f"Session {self.session_id}: Error sending buffered message: {e}")
                        break
                self.message_buffer.clear()
            
            return True
            
        except Exception as e:
            logger.error(f"Session {self.session_id}: Failed to initialize Pipecat: {e}")
            self.set_bridge_state(BridgeState.ERROR)
            return False
    
    def set_bridge_state(self, new_state: BridgeState):
        old_state = self.bridge_state
        self.bridge_state = new_state
        logger.info(f"Session {self.session_id}: Bridge state changed {old_state.value} → {new_state.value}")
    
    async def start_escalation(self) -> bool:
        try:
            if self.bridge_state != BridgeState.ACTIVE:
                logger.warning(f"Session {self.session_id}: Cannot start escalation")
                return False
            
            logger.info(f"Session {self.session_id}: Starting escalation process")
            self.set_bridge_state(BridgeState.ESCALATING)
            
            # Send stop message to Pipecat
            stop_msg = {
                "event": "stop",
                "streamSid": self.stream_sid
            }
            twilio_stop = self.talkdesk_to_twilio_format(stop_msg)
            await self.pipecat_conn.send_twilio_message(twilio_stop)
            
            # Close Pipecat connection
            await self.pipecat_conn.close()
            
            logger.info(f"Session {self.session_id}: Pipecat connection closed for escalation")
            
            self.escalation_event.set()
            await asyncio.sleep(2)
            
            self.set_bridge_state(BridgeState.PIPECAT_CLOSED)
            logger.info(f"Session {self.session_id}: Escalation ready")
            
            return True
            
        except Exception as e:
            logger.error(f"Session {self.session_id}: Error during escalation: {e}")
            return False
    
    async def complete_escalation(self, stop_msg: Dict[str, Any]) -> bool:
        try:
            logger.info(f"Session {self.session_id}: Completing escalation")
            
            await self.talkdesk_ws.send_text(json.dumps(stop_msg))
            logger.info(f"Session {self.session_id}: Escalation message sent to Talkdesk")
            
            self.set_bridge_state(BridgeState.CLOSING)
            return True
            
        except Exception as e:
            logger.error(f"Session {self.session_id}: Error completing escalation: {e}")
            return False
    
    async def start(self):
        """Start the bridge session"""
        try:
            logger.info(f"Starting Twilio bridge session: {self.session_id}")
            
            self.is_active = True
            self.set_bridge_state(BridgeState.WAITING_START)
            
            # Start message forwarding tasks
            forward_task = asyncio.create_task(self._forward_talkdesk_to_pipecat())
            backward_task = None
            
            self.tasks = {forward_task}
            
            while self.is_active and self.bridge_state not in [BridgeState.CLOSING, BridgeState.CLOSED]:
                # After Pipecat is initialized, add backward task
                if self.bridge_state == BridgeState.ACTIVE and backward_task is None:
                    backward_task = asyncio.create_task(self._forward_pipecat_to_talkdesk())
                    self.tasks.add(backward_task)
                    logger.info(f"Session {self.session_id}: Started Pipecat → Talkdesk forwarding")
                
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
                    logger.info(f"Session {self.session_id}: Normal termination")
                    break
                    
            for task in self.tasks:
                if not task.done():
                    task.cancel()
                    
        except Exception as e:
            logger.error(f"Session {self.session_id} error: {e}")
            self.stats['errors'] += 1
        finally:
            await self.stop()
    
    async def _forward_talkdesk_to_pipecat(self):
        """Forward messages from Talkdesk to Pipecat (Twilio format)"""
        logger.info(f"Session {self.session_id}: Starting Talkdesk → Pipecat forwarding")
        
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
                        logger.info(f"Session {self.session_id}: Received START from Talkdesk")
                        
                        # Extract session data
                        self.stream_sid = data.get('streamSid')
                        if not self.stream_sid and 'start' in data:
                            self.stream_sid = data['start'].get('streamSid')
                        
                        if 'start' in data:
                            self.interaction_id = data['start'].get('customParameters', {}).get('interaction_id')
                            
                            custom_params = data['start'].get('customParameters', {})
                            business_hours = custom_params.get('business_hours', '')
                            self.caller_id = custom_params.get('caller_id', '')
                            
                            logger.info(f"[{self.session_id}] Business hours: {business_hours}")
                            logger.info(f"[{self.session_id}] Caller ID: {self.caller_id}")
                            
                            # Extract business status
                            self.business_status = self.extract_business_status(business_hours)
                            
                            # Initialize Pipecat Twilio WebSocket
                            pipecat_initialized = await self.initialize_pipecat_with_business_status(self.business_status)
                            
                            if not pipecat_initialized:
                                logger.error(f"Failed to initialize Pipecat for session {self.session_id}")
                                break
                        
                        # Store session data
                        if self.stream_sid:
                            ACTIVE_SESSIONS[self.stream_sid] = self
                        
                        # Save to Redis
                        if self.pipecat_conn.call_id:
                            redis_client.hset(
                                self.pipecat_conn.call_id, 
                                mapping={
                                    "interaction_id": self.interaction_id, 
                                    "stream_sid": self.stream_sid,
                                    "caller_id": self.caller_id
                                }
                            )
                        
                        # Convert and send to Pipecat
                        twilio_msg = self.talkdesk_to_twilio_format(data)
                        await self.pipecat_conn.send_twilio_message(twilio_msg)
                        self.stats['talkdesk_to_pipecat_messages'] += 1
                        
                        continue
                        
                    elif event == 'stop':
                        logger.info(f"Session {self.session_id}: Received STOP from Talkdesk")
                        
                        # Save to MySQL
                        try:
                            call_id = self.pipecat_conn.call_id
                            assistant_id = PIPECAT_AGENT_NAME
                            
                            mysql_success = await save_call_to_mysql(
                                call_id=call_id,
                                assistant_id=assistant_id,
                                interaction_id=self.interaction_id,
                                phone_number=self.caller_id
                            )
                            
                            if mysql_success:
                                logger.info(f"MySQL save successful for call {call_id}")
                            else:
                                logger.error(f"MySQL save failed for call {call_id}")
                                
                        except Exception as mysql_error:
                            logger.error(f"MySQL save error: {str(mysql_error)}")
                        
                        # Send stop to Pipecat
                        twilio_msg = self.talkdesk_to_twilio_format(data)
                        await self.pipecat_conn.send_twilio_message(twilio_msg)
                        
                        break
                        
                    elif event == 'media':
                        # Handle media messages
                        if self.bridge_state == BridgeState.WAITING_START:
                            # Buffer messages until Pipecat is ready
                            self.message_buffer.append(data)
                            if len(self.message_buffer) > 100:
                                self.message_buffer.pop(0)
                            logger.debug(f"Session {self.session_id}: Buffered message (buffer size: {len(self.message_buffer)})")
                                
                        elif self.bridge_state == BridgeState.ACTIVE:
                            # Forward to Pipecat
                            twilio_msg = self.talkdesk_to_twilio_format(data)
                            await self.pipecat_conn.send_twilio_message(twilio_msg)
                            self.stats['talkdesk_to_pipecat_messages'] += 1
                    
                    else:
                        # Handle other message types
                        if self.bridge_state == BridgeState.ACTIVE:
                            twilio_msg = self.talkdesk_to_twilio_format(data)
                            await self.pipecat_conn.send_twilio_message(twilio_msg)
                            
                except json.JSONDecodeError:
                    logger.error(f"Session {self.session_id}: Invalid JSON from Talkdesk")
                except Exception as e:
                    logger.error(f"Session {self.session_id}: Error processing Talkdesk message: {e}")
                    self.stats['errors'] += 1
                    
        except Exception as e:
            logger.error(f"Session {self.session_id}: Forward error: {e}")
            self.stats['errors'] += 1
    
    async def _forward_pipecat_to_talkdesk(self):
        """Forward messages from Pipecat to Talkdesk"""
        logger.info(f"Session {self.session_id}: Starting Pipecat → Talkdesk forwarding")
        
        try:
            while self.is_active:
                try:
                    if self.bridge_state in [BridgeState.ESCALATING, BridgeState.PIPECAT_CLOSED]:
                        logger.info(f"Session {self.session_id}: Pipecat forwarding paused")
                        while (self.bridge_state in [BridgeState.ESCALATING, BridgeState.PIPECAT_CLOSED] 
                               and self.is_active):
                            await asyncio.sleep(0.5)
                        continue
                    
                    if self.bridge_state != BridgeState.ACTIVE:
                        break
                        
                    # Receive message from Pipecat
                    pipecat_msg = await self.pipecat_conn.receive_twilio_message()
                    
                    if pipecat_msg is None:
                        # Connection closed or no message
                        continue
                        
                    # Convert Twilio format back to Talkdesk format and send
                    if pipecat_msg.get('event') == 'media':
                        talkdesk_msg = self.twilio_to_talkdesk_format(pipecat_msg)
                        await self.talkdesk_ws.send_text(json.dumps(talkdesk_msg))
                        self.stats['pipecat_to_talkdesk_messages'] += 1
                        
                        if self.stats['pipecat_to_talkdesk_messages'] == 1:
                            logger.info(f"First message from Pipecat to Talkdesk")
                    
                    # Handle other message types from Pipecat if needed
                    elif pipecat_msg.get('event') in ['mark', 'stop']:
                        logger.debug(f"Received {pipecat_msg.get('event')} from Pipecat")
                        
                except Exception as e:
                    if self.bridge_state == BridgeState.ACTIVE:
                        logger.error(f"Session {self.session_id}: Backward error: {e}")
                        self.stats['errors'] += 1
                        break
                    else:
                        logger.debug(f"Session {self.session_id}: Pipecat error during escalation: {e}")
                        break
                        
        except Exception as e:
            logger.error(f"Session {self.session_id}: Fatal backward error: {e}")
            self.stats['errors'] += 1
    
    async def stop(self):
        logger.info(f"Stopping session {self.session_id}")
        self.is_active = False
        self.set_bridge_state(BridgeState.CLOSED)
        
        logger.info(f"Session {self.session_id} stats: "
                   f"Talkdesk→Pipecat: {self.stats['talkdesk_to_pipecat_messages']}, "
                   f"Pipecat→Talkdesk: {self.stats['pipecat_to_talkdesk_messages']}, "
                   f"Errors: {self.stats['errors']}")
        
        # Close Pipecat connection
        if self.pipecat_conn.state not in [ConnectionState.CLOSED, ConnectionState.CLOSING]:
            await self.pipecat_conn.close()
        
        # Close Talkdesk connection
        if self.bridge_state != BridgeState.PIPECAT_CLOSED:
            try:
                await self.talkdesk_ws.send_text(json.dumps({"event": "stop"}))
            except Exception:
                pass

##############################################
# FastAPI Application
##############################################
app = FastAPI()

@app.get("/healthz")
async def healthz():
    return {"status": "ok", "service": "pipecat-twilio-bridge"}

@app.websocket("/talkdesk")
async def talkdesk_ws(ws: WebSocket):
    await ws.accept()
    session_id = str(uuid.uuid4())
    logger.info(f"New Talkdesk connection - Session: {session_id}")
    session = BridgeSession(session_id, ws)
    ACTIVE_SESSIONS[session_id] = session

    try:
        await session.start()
    except WebSocketDisconnect:
        logger.info(f"Session {session_id} disconnected")
    finally:
        ACTIVE_SESSIONS.pop(session_id, None)
        logger.info(f"Session {session_id} ended")

async def call_pipecat_stat_internal(call_id: str, interaction_id: str) -> Optional[Dict[str, Any]]:
    """Call Pipecat statistics service (placeholder)"""
    try:
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

def limita_testo_256(text):
    max_length = 240
    if len(text) <= max_length:
        return text
    truncated = text[:max_length]
    last_space = truncated.rfind(" ")
    if last_space != -1:
        truncated = truncated[:last_space]
    return truncated.strip()

def build_talkdesk_message(stream_sid: str, pipecat_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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

@app.post("/escalation")
async def escalation(request: Request) -> Dict[str, Any]:
    """Endpoint for handling escalation"""
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
                    logger.info(f"Starting escalation for call {call_id}")
                    
                    escalation_started = await session.start_escalation()
                    
                    if not escalation_started:
                        raise Exception("Failed to start escalation process")
                    
                    logger.info(f"Pipecat session closed for call {call_id}")
                    
                    await asyncio.sleep(2)
                    
                    pipecat_data = await call_pipecat_stat_internal(call_id, interaction_id)
                    
                    stop_msg = build_talkdesk_message(stream_sid, pipecat_data)
                    
                    escalation_completed = await session.complete_escalation(stop_msg)
                    
                    if escalation_completed:
                        logger.info(f"Escalation completed successfully for {stream_sid}")
                    else:
                        raise Exception("Failed to complete escalation process")
                    
                except Exception as e:
                    logger.error(f"Error during escalation: {str(e)}")
                    
                    try:
                        logger.info(f"Attempting fallback escalation...")
                        stop_msg = build_talkdesk_message(stream_sid, None)
                        
                        if session.bridge_state not in [BridgeState.CLOSED]:
                            await session.talkdesk_ws.send_text(json.dumps(stop_msg))
                            logger.info(f"Fallback escalation sent")
                        else:
                            logger.error(f"Session already closed, cannot send fallback")
                            
                    except Exception as fallback_error:
                        logger.error(f"Fallback escalation failed: {str(fallback_error)}")
            else:
                logger.warning(f"Escalation: no live session for streamSid {stream_sid}")
        else:
            logger.warning(f"Escalation: streamSid not found for call_id {call_id}")
    else:
        logger.warning("Escalation: call_id missing")
    
    return {"results": results}

if __name__ == "__main__":
    import uvicorn
    import os
    port = int(os.environ.get("PORT", 8000))  # Azure uses PORT env var
    uvicorn.run(app, host="0.0.0.0", port=port)