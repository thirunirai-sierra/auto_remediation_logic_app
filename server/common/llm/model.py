import os

from dotenv import load_dotenv
from gen_ai_hub.proxy.langchain.openai import ChatOpenAI

load_dotenv()


def get_llm(temperature: float = 0.0, deployment_id: str | None = None) -> ChatOpenAI:
    deployment = deployment_id or os.getenv("AICORE_CHAT_DEPLOYMENT_ID", "").strip()
    if not deployment:
        raise ValueError("AICORE_CHAT_DEPLOYMENT_ID is not configured")
    return ChatOpenAI(deployment_id=deployment, temperature=temperature)
