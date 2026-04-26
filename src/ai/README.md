# MediaCircle AI Classification Module

## Overview

The AI module provides intelligent classification of search queries for the MediaCircle platform. It uses OpenAI's GPT-4o-mini model to categorize user search inputs into structured classifications.

## Architecture

```
ai/
├── providers/openai.py      # OpenAI API wrapper with caching
├── prompts/classifier.py    # Search query classifier
└── tests/classifier.py      # Comprehensive test suite
```

## Usage

```python
from ai.prompts.classifier import classify

result = await classify("Tom Hanks action movies")
```

## Testing

```bash
# Run all tests
python -m ai.tests.classifier

# Run single test
python -m ai.tests.classifier "Tom Hanks movies"
```

## Configuration

Required environment variables:
- OPENAI_API_KEY
- OPENAI_ORGANIZATION (optional)
