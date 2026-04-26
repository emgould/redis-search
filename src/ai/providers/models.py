
from enum import StrEnum


class CerebrasModels(StrEnum):
    LLAMA_70B = "llama3.3-70b"
    LLAMA_8B = "llama3.1-8b"
    QWEN_32B = "qwen-3-32b"
    GPT_OSS = "gpt-oss-120b"


class OpenAIModels(StrEnum):
    GPT_5_5 = "gpt-5.5"
