from enum import StrEnum


class CerebrasModels(StrEnum):
    LLAMA_70B = "llama3.3-70b"
    LLAMA_8B = "llama3.1-8b"
    QWEN_32B = "qwen-3-32b"
    GPT_OSS = "gpt-oss-120b"


class OpenAIModels(StrEnum):
    GPT_5_6_TERRA = "gpt-5.6-terra"
    GPT_5_6_LUNA = "gpt-5.6-luna"
    GPT_6_1_SOL = "gpt-6.1-sol"
