"""
FastAPI application module
"""
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
import logging
from fastapi.responses import JSONResponse

from agent.agent import Agent
from config.config import Config
from api.routers.agent import router as agent_router
from api.wsrouters.webs2s import router as websocket_router
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import time
from api.auth import decode_token

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):

    logger.info("Starting Alees AI Agent Runtime")

    # Load configuration
    config = Config()

    # Create agent runtime
    agent = Agent(config)
   
    try:
        # 3. Initialize the global session/client
        await agent.session.initialize()
        
        # We link the session here so that any route using 'get_agent' 
        for tool in agent.session.tool_registry.get_tools():
            if hasattr(tool, 'session'):
                tool.session = agent.session
        
        # 4. Store inside FastAPI app state for global access
        app.state.sessions = {}
        app.state.config = config
        app.state.agent = agent
        # app.state.pending_approvals = agent.session.pending_approvals
        logger.info("Agent initialized and Tools linked successfully")
        # logger.info(f"Approval policy: {config.approval}")
        # BACKGROUND CLEANUP TASK
        async def cleanup_sessions():
            while True:
                await asyncio.sleep(60)  # run every 1 minute

                now = time.time()
                sessions = app.state.sessions

                to_delete = []

                for uid, sess in sessions.items():
                    if not hasattr(sess, "last_activity"):
                        continue

                    if now - sess.last_activity > 300:  # 5 min idle
                        print(f"🧹 Cleaning session: {uid}")
                        
                        if sess.client:
                            await sess.client.close()

                        to_delete.append(uid)

                for uid in to_delete:
                    del sessions[uid]

        # start background task
        asyncio.create_task(cleanup_sessions())
        
    except Exception as e:
        logger.error(f"Critical Failure during Agent Startup: {e}")
        raise e

    yield

    logger.info("Shutting down agent runtime")

    # Optional cleanup
    if agent.session and agent.session.client:
        await agent.session.client.close()


# Create FastAPI application
def create_app():

    app = FastAPI(
        title="AI Agent API",
        version="1.0",
        lifespan=lifespan
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
  
    @app.middleware("http")
    async def auth_middleware(request: Request, call_next):

        public_paths = ["/docs", "/openapi.json"]

        if request.url.path.startswith(tuple(public_paths)):
            return await call_next(request)

        auth_header = request.headers.get("Authorization")

        if not auth_header or not auth_header.startswith("Bearer "):
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing authentication token"}
            )

        token = auth_header.split(" ")[1]

        try:
            user = decode_token(token)

            request.state.user = user
            request.state.token = token

        except Exception as e:
            print("JWT ERROR:", str(e))

            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or expired token"}
            )

        response = await call_next(request)

        return response

    app.include_router(agent_router, prefix="/api")
    app.include_router(websocket_router)

    
    return app


# Global app instance
app = create_app()


# Helper function to get agent
def get_agent(request: Request) -> Agent:
    return request.app.state.agent