#!/usr/bin/env python3
"""
Generate mock data from real NewsAPI responses.

This utility fetches real data from NewsAPI and saves it as JSON fixtures
that can be used in tests. This ensures test data matches actual API responses.

Note: Caching is automatically disabled by setting ENVIRONMENT=test to ensure
fresh data is fetched from the API.

Usage:
    python generate_mock_data.py              # Generate search mock data (default)
    python generate_mock_data.py --all        # Generate all mock data (core, handlers, wrappers, models, search, endpoints)
    python generate_mock_data.py --core       # Generate core method results (core/)
    python generate_mock_data.py --handlers   # Generate handler method results (handlers/)
    python generate_mock_data.py --wrappers   # Generate wrapper method results (wrappers/)
    python generate_mock_data.py --models     # Generate model instantiation results (models/)
    python generate_mock_data.py --search     # Generate search method results (search/)
    python generate_mock_data.py --endpoints  # Generate endpoint mocking for search.py (make_requests/)

Requirements:
    - NEWS_API_KEY environment variable must be set
    - Internet connection to access NewsAPI
"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any
from unittest.mock import Mock

from firebase_functions import https_fn

# Add python_functions directory to path to import services
python_functions_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(python_functions_dir))

from api.news.core import NewsService  # noqa: E402
from api.news.handlers import NewsHandler  # noqa: E402
from api.news.models import (  # noqa: E402
    MCNewsItem,
    NewsSearchResponse,
    NewsSource,
    NewsSourcesResponse,
    TrendingNewsResponse,
)
from api.news.search import NewsSearchService  # noqa: E402
from api.news.wrappers import (  # noqa: E402
    get_news_sources_async,
    get_trending_news_async,
    search_news_async,
)
from utils.cache import EnhancedJSONEncoder, disable_cache  # noqa: E402

# Set environment to test mode FIRST to disable caching
os.environ["ENVIRONMENT"] = "test"

# Test data configuration - Well-known, stable news queries
TEST_DATA_CONFIG = {
    "country": "us",
    "category": "technology",
    "query": "artificial intelligence",
    "query2": "technology",
    "language": "en",
}

# Core service methods to generate mock data for
# Format: [method_name, kwargs, output_filename]
TEST_CORE = [
    [
        "_process_article_item",
        {
            "article_data": {
                "source": {"id": "bbc-news", "name": "BBC News"},
                "author": "Test Author",
                "title": "Test Article Title",
                "description": "Test article description",
                "url": "https://example.com/article",
                "urlToImage": "https://example.com/image.jpg",
                "publishedAt": "2024-01-15T10:30:00Z",
                "content": "Test article content...",
            }
        },
        "process_article_item",
    ],
]

# Handler methods to generate mock data for
# Format: [method_name, mock_request_params, output_filename]
TEST_HANDLERS = [
    [
        "get_trending_news",
        {"country": "us", "page_size": "10"},
        "handler_get_trending_news",
    ],
    [
        "search_news",
        {"query": "artificial intelligence", "page_size": "10"},
        "handler_search_news",
    ],
    [
        "get_news_sources",
        {"language": "en"},
        "handler_get_news_sources",
    ],
]

# Wrapper methods to generate mock data for
# Format: [function_name, kwargs, output_filename]
TEST_WRAPPERS = [
    [
        "get_trending_news_async",
        {"country": "us", "page_size": 10},
        "wrapper_get_trending_news",
    ],
    [
        "search_news_async",
        {"query": "artificial intelligence", "page_size": 10},
        "wrapper_search_news",
    ],
    [
        "get_news_sources_async",
        {"language": "en"},
        "wrapper_get_news_sources",
    ],
]

# Model instantiation tests
# Format: [model_class, sample_data, output_filename]
TEST_MODELS = [
    [
        "MCNewsItem",
        {
            "title": "Test Article",
            "description": "Test description",
            "url": "https://example.com/article",
            "source": {"id": "test", "name": "Test Source"},
            "publishedAt": "2024-01-15T10:30:00Z",
        },
        "model_news_article",
    ],
    [
        "TrendingNewsResponse",
        {
            "articles": [
                {
                    "title": "Test Article",
                    "url": "https://example.com/article",
                    "source": {"id": "test", "name": "Test Source"},
                }
            ],
            "total_results": 1,
            "country": "us",
            "status": "ok",
        },
        "model_trending_news_response",
    ],
    [
        "NewsSearchResponse",
        {
            "articles": [
                {
                    "title": "Test Article",
                    "url": "https://example.com/article",
                    "source": {"id": "test", "name": "Test Source"},
                }
            ],
            "total_results": 1,
            "query": "test",
            "status": "ok",
        },
        "model_search_news_response",
    ],
    [
        "NewsSourcesResponse",
        {
            "sources": [
                {
                    "id": "test",
                    "name": "Test Source",
                    "category": "technology",
                    "language": "en",
                    "country": "us",
                }
            ],
            "total_sources": 1,
            "status": "ok",
        },
        "model_news_sources_response",
    ],
]

# API endpoints to test directly (for search.py)
# Format: [endpoint_name, params_dict, function_name, mock_name]
TEST_ENDPOINTS = [
    # NewsAPI endpoints (via newsapi client)
    [
        "get_top_headlines",
        {"country": "us", "page_size": 10},
        "get_trending_news",
        "trending_news_us",
    ],
    [
        "get_top_headlines",
        {"country": "us", "category": "technology", "page_size": 10},
        "get_trending_news",
        "trending_news_technology",
    ],
    [
        "get_top_headlines",
        {"country": "us", "q": "technology", "page_size": 10},
        "get_trending_news",
        "trending_news_query",
    ],
    [
        "get_everything",
        {"q": "artificial intelligence", "language": "en", "page_size": 10},
        "search_news",
        "search_news_ai",
    ],
    [
        "get_everything",
        {"q": "technology", "language": "en", "sort_by": "publishedAt", "page_size": 10},
        "search_news",
        "search_news_technology",
    ],
    [
        "get_sources",
        {"language": "en"},
        "get_news_sources",
        "news_sources_en",
    ],
    [
        "get_sources",
        {"category": "technology", "language": "en"},
        "get_news_sources",
        "news_sources_tech",
    ],
]

# Search service methods to generate mock data for
# Format: [method_name, kwargs, output_filename]
TEST_SEARCH = [
    [
        "get_trending_news",
        {"country": "us", "page_size": 10},
        "search_get_trending_news",
    ],
    [
        "get_trending_news",
        {"country": "us", "category": "technology", "page_size": 10},
        "search_get_trending_news_category",
    ],
    [
        "search_news",
        {"query": "artificial intelligence", "page_size": 10},
        "search_search_news",
    ],
    [
        "get_news_sources",
        {"language": "en"},
        "search_get_news_sources",
    ],
]


class NewsMockDataGenerator:
    """Generate mock data from real NewsAPI responses."""

    def __init__(self, api_key: str):
        """Initialize generator with API credentials.

        Args:
            api_key: NewsAPI key
        """
        self.api_key = api_key

        # Set up directories
        self.core_dir = Path.cwd() / "fixtures" / "core"
        self.core_dir.mkdir(parents=True, exist_ok=True)
        self.handlers_dir = Path.cwd() / "fixtures" / "handlers"
        self.handlers_dir.mkdir(parents=True, exist_ok=True)
        self.wrappers_dir = Path.cwd() / "fixtures" / "wrappers"
        self.wrappers_dir.mkdir(parents=True, exist_ok=True)
        self.models_dir = Path.cwd() / "fixtures" / "models"
        self.models_dir.mkdir(parents=True, exist_ok=True)
        self.search_dir = Path.cwd() / "fixtures" / "search"
        self.search_dir.mkdir(parents=True, exist_ok=True)
        self.make_requests_dir = Path.cwd() / "fixtures" / "make_requests"
        self.make_requests_dir.mkdir(parents=True, exist_ok=True)

        disable_cache()

    def _save_json(self, filename: str, data: Any, directory: Path) -> None:
        """Save data to JSON file.

        Args:
            filename: Output filename
            data: Data to save (will be converted to dict if it's a Pydantic model)
            directory: Target directory
        """
        output_path = directory / filename

        with open(output_path, "w") as f:
            json.dump(data, f, indent=2, cls=EnhancedJSONEncoder)
        print(f"  → Saved to {output_path}")

    async def generate_core_methods(self) -> None:
        """Generate mock data by testing NewsService core methods.

        This method tests the core service methods and basic functionality.
        """
        print("\n" + "=" * 60)
        print("News Core Methods Mock Data Generator")
        print("=" * 60 + "\n")

        print(f"Total methods to process: {len(TEST_CORE)}\n")

        # Create NewsService instance (will use API key from environment)
        service = NewsService()

        success_count = 0
        error_count = 0
        errors = []

        for idx, method_config in enumerate(TEST_CORE, 1):
            method_name = method_config[0]
            kwargs = method_config[1]
            output_filename = method_config[2]

            try:
                print(f"[{idx}/{len(TEST_CORE)}] Processing: {method_name}")
                print(f"  Parameters: {list(kwargs.keys())}")

                # Get the method from the service
                method = getattr(service, method_name)

                # Call the method with kwargs
                result = method(**kwargs)

                # Convert Pydantic model to dict if needed
                if hasattr(result, "model_dump"):
                    result = result.model_dump()
                elif hasattr(result, "dict"):
                    result = result.dict()

                # Generate filename and save
                filename = f"{output_filename}.json"
                self._save_json(filename, result, directory=self.core_dir)

                success_count += 1
                print("  ✓ Success\n")

            except Exception as e:
                error_count += 1
                error_msg = f"  ❌ Error: {str(e)}\n"
                print(error_msg)
                errors.append((method_name, str(e)))

        # Print summary
        print("=" * 60)
        print("Generation Summary")
        print("=" * 60)
        print(f"✓ Successful: {success_count}/{len(TEST_CORE)}")
        if error_count > 0:
            print(f"❌ Failed: {error_count}/{len(TEST_CORE)}")

        if errors:
            print("\nErrors encountered:")
            for method, error in errors:
                print(f"  - {method}: {error}")

        print(f"\n✓ Files saved to: {self.core_dir}")
        print("=" * 60 + "\n")

        if error_count > 0:
            raise Exception(f"Failed to generate {error_count} core method mocks")

    async def generate_handlers_methods(self) -> None:
        """Generate mock data by executing NewsHandler methods.

        This method calls the actual NewsHandler methods defined in TEST_HANDLERS
        and saves the results as mock data files in the handlers directory.
        """
        print("\n" + "=" * 60)
        print("News Handler Methods Mock Data Generator")
        print("=" * 60 + "\n")

        print(f"Total methods to process: {len(TEST_HANDLERS)}\n")

        # Create NewsHandler instance
        handler = NewsHandler()

        success_count = 0
        error_count = 0
        errors = []

        for idx, method_config in enumerate(TEST_HANDLERS, 1):
            method_name = method_config[0]
            mock_params = method_config[1]
            output_filename = method_config[2]

            try:
                print(f"[{idx}/{len(TEST_HANDLERS)}] Processing: {method_name}")
                print(f"  Parameters: {mock_params}")

                # Create a mock request object
                mock_request = Mock(spec=https_fn.Request)
                mock_request.args = Mock()
                mock_request.args.get = lambda key, default=None: mock_params.get(key, default)

                # Get the method from the handler
                method = getattr(handler, method_name)

                # Call the method with mock request
                response = await method(mock_request)

                # Extract response data from https_fn.Response
                if hasattr(response, "data"):
                    if isinstance(response.data, bytes):
                        result = json.loads(response.data.decode("utf-8"))
                    elif isinstance(response.data, str):
                        result = json.loads(response.data)
                    else:
                        result = json.loads(str(response.data))
                else:
                    raise ValueError("Unable to extract data from response object")

                # Generate filename and save
                filename = f"{output_filename}.json"
                self._save_json(filename, result, directory=self.handlers_dir)

                success_count += 1
                print("  ✓ Success\n")

            except Exception as e:
                error_count += 1
                error_msg = f"  ❌ Error: {str(e)}\n"
                print(error_msg)
                errors.append((method_name, str(e)))

        # Print summary
        print("=" * 60)
        print("Generation Summary")
        print("=" * 60)
        print(f"✓ Successful: {success_count}/{len(TEST_HANDLERS)}")
        if error_count > 0:
            print(f"❌ Failed: {error_count}/{len(TEST_HANDLERS)}")

        if errors:
            print("\nErrors encountered:")
            for method, error in errors:
                print(f"  - {method}: {error}")

        print(f"\n✓ Files saved to: {self.handlers_dir}")
        print("=" * 60 + "\n")

        if error_count > 0:
            raise Exception(f"Failed to generate {error_count} handler method mocks")

    async def generate_wrappers_methods(self) -> None:
        """Generate mock data by executing wrapper functions.

        This method calls the actual wrapper functions defined in TEST_WRAPPERS
        and saves the results as mock data files in the wrappers directory.
        """
        print("\n" + "=" * 60)
        print("News Wrapper Methods Mock Data Generator")
        print("=" * 60 + "\n")

        print(f"Total methods to process: {len(TEST_WRAPPERS)}\n")

        success_count = 0
        error_count = 0
        errors = []

        for idx, method_config in enumerate(TEST_WRAPPERS, 1):
            function_name = method_config[0]
            kwargs = method_config[1]
            output_filename = method_config[2]

            try:
                print(f"[{idx}/{len(TEST_WRAPPERS)}] Processing: {function_name}")
                print(f"  Parameters: {kwargs}")

                # Get the function from globals
                if function_name == "get_trending_news_async":
                    func = get_trending_news_async
                elif function_name == "search_news_async":
                    func = search_news_async
                elif function_name == "get_news_sources_async":
                    func = get_news_sources_async
                else:
                    raise ValueError(f"Unknown wrapper function: {function_name}")

                # Call the function with kwargs
                result, error = await func(**kwargs)

                if error:
                    raise Exception(f"Function returned error: {error}")

                # Generate filename and save
                filename = f"{output_filename}.json"
                self._save_json(filename, result, directory=self.wrappers_dir)

                success_count += 1
                print("  ✓ Success\n")

            except Exception as e:
                error_count += 1
                error_msg = f"  ❌ Error: {str(e)}\n"
                print(error_msg)
                errors.append((function_name, str(e)))

        # Print summary
        print("=" * 60)
        print("Generation Summary")
        print("=" * 60)
        print(f"✓ Successful: {success_count}/{len(TEST_WRAPPERS)}")
        if error_count > 0:
            print(f"❌ Failed: {error_count}/{len(TEST_WRAPPERS)}")

        if errors:
            print("\nErrors encountered:")
            for function, error in errors:
                print(f"  - {function}: {error}")

        print(f"\n✓ Files saved to: {self.wrappers_dir}")
        print("=" * 60 + "\n")

        if error_count > 0:
            raise Exception(f"Failed to generate {error_count} wrapper method mocks")

    async def generate_models_methods(self) -> None:
        """Generate mock data by instantiating Pydantic models.

        This method instantiates models defined in TEST_MODELS with sample data
        and saves the results as mock data files in the models directory.
        """
        print("\n" + "=" * 60)
        print("News Models Mock Data Generator")
        print("=" * 60 + "\n")

        print(f"Total models to process: {len(TEST_MODELS)}\n")

        success_count = 0
        error_count = 0
        errors = []

        for idx, model_config in enumerate(TEST_MODELS, 1):
            model_class_name = model_config[0]
            sample_data = model_config[1]
            output_filename = model_config[2]

            try:
                print(f"[{idx}/{len(TEST_MODELS)}] Processing: {model_class_name}")
                print(f"  Sample data keys: {list(sample_data.keys())}")

                # Get the model class
                if model_class_name == "MCNewsItem":
                    model_class = MCNewsItem
                elif model_class_name == "TrendingNewsResponse":
                    model_class = TrendingNewsResponse
                elif model_class_name == "NewsSearchResponse":
                    model_class = NewsSearchResponse
                elif model_class_name == "NewsSourcesResponse":
                    model_class = NewsSourcesResponse
                else:
                    raise ValueError(f"Unknown model class: {model_class_name}")

                # Handle nested models
                if model_class_name == "MCNewsItem" and "source" in sample_data:
                    # MCNewsItem expects NewsSource (not NewsSourceDetails)
                    source_data = sample_data["source"]
                    if isinstance(source_data, dict):
                        sample_data["source"] = NewsSource(**source_data)

                # Handle nested articles in response models
                elif model_class_name in ["TrendingNewsResponse", "NewsSearchResponse"]:
                    if "articles" in sample_data and isinstance(sample_data["articles"], list):
                        processed_articles = []
                        for article_data in sample_data["articles"]:
                            if isinstance(article_data, dict) and "source" in article_data:
                                source_data = article_data["source"]
                                if isinstance(source_data, dict):
                                    article_data["source"] = NewsSource(**source_data)
                            processed_articles.append(article_data)
                        sample_data["articles"] = processed_articles

                # Instantiate the model
                model_instance = model_class(**sample_data)

                # Convert to dict for JSON serialization
                if hasattr(model_instance, "model_dump"):
                    result = model_instance.model_dump()
                elif hasattr(model_instance, "dict"):
                    result = model_instance.dict()
                else:
                    result = model_instance.__dict__

                # Generate filename and save
                filename = f"{output_filename}.json"
                self._save_json(filename, result, directory=self.models_dir)

                success_count += 1
                print("  ✓ Success\n")

            except Exception as e:
                error_count += 1
                error_msg = f"  ❌ Error: {str(e)}\n"
                print(error_msg)
                errors.append((model_class_name, str(e)))

        # Print summary
        print("=" * 60)
        print("Generation Summary")
        print("=" * 60)
        print(f"✓ Successful: {success_count}/{len(TEST_MODELS)}")
        if error_count > 0:
            print(f"❌ Failed: {error_count}/{len(TEST_MODELS)}")

        if errors:
            print("\nErrors encountered:")
            for model, error in errors:
                print(f"  - {model}: {error}")

        print(f"\n✓ Files saved to: {self.models_dir}")
        print("=" * 60 + "\n")

        if error_count > 0:
            raise Exception(f"Failed to generate {error_count} model mocks")

    async def generate_all_endpoints(self) -> None:
        """Generate mock data for all test endpoints.

        This method executes all endpoints defined in TEST_ENDPOINTS using the
        test data provided in each endpoint configuration and saves the responses
        as mock data files in the make_requests directory.
        """
        print("\n" + "=" * 60)
        print("News Mock Data Generator - All Endpoints (search.py)")
        print("=" * 60 + "\n")

        print(f"Total endpoints to process: {len(TEST_ENDPOINTS)}\n")

        # Create NewsSearchService instance to access newsapi client
        service = NewsSearchService()
        # Trigger property access to initialize newsapi
        _ = service.newsapi

        success_count = 0
        error_count = 0
        errors = []

        for idx, endpoint_config in enumerate(TEST_ENDPOINTS, 1):
            # Unpack the endpoint configuration
            endpoint_name = endpoint_config[0]
            params = endpoint_config[1]
            function_name = endpoint_config[2]
            mock_name = endpoint_config[3]

            try:
                print(f"[{idx}/{len(TEST_ENDPOINTS)}] Processing: {function_name}")
                print(f"  Endpoint: {endpoint_name}")
                print(f"  Parameters: {params}")

                # Get the method from newsapi client
                method = getattr(service.newsapi, endpoint_name)

                # Call the method with params
                data = method(**params)

                # Generate filename and save
                filename = mock_name + ".json"
                self._save_json(filename, data, directory=self.make_requests_dir)

                success_count += 1
                print("  ✓ Success\n")

            except Exception as e:
                error_count += 1
                error_msg = f"  ❌ Error: {str(e)}\n"
                print(error_msg)
                errors.append((endpoint_name, str(e)))

        # Print summary
        print("=" * 60)
        print("Generation Summary")
        print("=" * 60)
        print(f"✓ Successful: {success_count}/{len(TEST_ENDPOINTS)}")
        if error_count > 0:
            print(f"❌ Failed: {error_count}/{len(TEST_ENDPOINTS)}")

        if errors:
            print("\nErrors encountered:")
            for endpoint, error in errors:
                print(f"  - {endpoint}: {error}")

        print(f"\n✓ Files saved to: {self.make_requests_dir}")
        print("=" * 60 + "\n")

        if error_count > 0:
            raise Exception(f"Failed to generate {error_count} endpoint mocks")

    async def generate_search_methods(self) -> None:
        """Generate mock data by executing NewsSearchService methods.

        This method calls the actual NewsSearchService methods defined in TEST_SEARCH
        and saves the results as mock data files in the search directory.
        """
        print("\n" + "=" * 60)
        print("News Search Methods Mock Data Generator")
        print("=" * 60 + "\n")

        print(f"Total methods to process: {len(TEST_SEARCH)}\n")

        # Create NewsSearchService instance
        service = NewsSearchService()

        success_count = 0
        error_count = 0
        errors = []

        for idx, method_config in enumerate(TEST_SEARCH, 1):
            # Unpack the method configuration
            method_name = method_config[0]
            kwargs = method_config[1]
            output_filename = method_config[2]

            try:
                print(f"[{idx}/{len(TEST_SEARCH)}] Processing: {method_name}")
                print(f"  Parameters: {kwargs}")

                # Get the method from the service
                method = getattr(service, method_name)

                # Call the method with kwargs
                result = await method(**kwargs)

                # Convert Pydantic model to dict
                if hasattr(result, "model_dump"):
                    result = result.model_dump()
                elif hasattr(result, "dict"):
                    result = result.dict()

                # Generate filename and save
                filename = f"{output_filename}.json"
                self._save_json(filename, result, directory=self.search_dir)

                success_count += 1
                print("  ✓ Success\n")

            except Exception as e:
                error_count += 1
                error_msg = f"  ❌ Error: {str(e)}\n"
                print(error_msg)
                errors.append((method_name, str(e)))

        # Print summary
        print("=" * 60)
        print("Generation Summary")
        print("=" * 60)
        print(f"✓ Successful: {success_count}/{len(TEST_SEARCH)}")
        if error_count > 0:
            print(f"❌ Failed: {error_count}/{len(TEST_SEARCH)}")

        if errors:
            print("\nErrors encountered:")
            for method, error in errors:
                print(f"  - {method}: {error}")

        print(f"\n✓ Files saved to: {self.search_dir}")
        print("=" * 60 + "\n")

        if error_count > 0:
            raise Exception(f"Failed to generate {error_count} search method mocks")

    async def generate_all(self) -> None:
        """Generate all mock data files."""
        print("\n" + "=" * 60)
        print("News Mock Data Generator")
        print("=" * 60 + "\n")

        # Generate all types
        await self.generate_core_methods()
        await self.generate_handlers_methods()
        await self.generate_wrappers_methods()
        await self.generate_models_methods()
        await self.generate_all_endpoints()  # Endpoint mocking for search.py
        await self.generate_search_methods()

        print("\n" + "=" * 60)
        print("✓ Mock data generation complete!")
        print(f"✓ Core method files saved to: {self.core_dir}")
        print(f"✓ Handler method files saved to: {self.handlers_dir}")
        print(f"✓ Wrapper method files saved to: {self.wrappers_dir}")
        print(f"✓ Model files saved to: {self.models_dir}")
        print(f"✓ Endpoint files saved to: {self.make_requests_dir}")
        print(f"✓ Search method files saved to: {self.search_dir}")
        print("=" * 60 + "\n")


async def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Generate mock data from real NewsAPI responses")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate all mock data (core, handlers, wrappers, models, search)",
    )
    parser.add_argument(
        "--core",
        action="store_true",
        help="Generate mock data by executing core service methods",
    )
    parser.add_argument(
        "--handlers",
        action="store_true",
        help="Generate mock data by executing handler methods",
    )
    parser.add_argument(
        "--wrappers",
        action="store_true",
        help="Generate mock data by executing wrapper functions",
    )
    parser.add_argument(
        "--models",
        action="store_true",
        help="Generate mock data by instantiating models",
    )
    parser.add_argument(
        "--search",
        action="store_true",
        help="Generate mock data by executing search service methods",
    )
    parser.add_argument(
        "--endpoints",
        action="store_true",
        help="Generate mock data for all test endpoints (make_requests/)",
    )
    args = parser.parse_args()

    # Get API credentials from environment
    api_key = os.getenv("NEWS_API_KEY")

    if not api_key:
        print("ERROR: Missing required environment variable: NEWS_API_KEY")
        print("\nPlease set it with:")
        print("  export NEWS_API_KEY='your_key_here'")
        return 1

    try:
        generator = NewsMockDataGenerator(api_key)

        if args.core:
            # Generate mock data by executing core service methods
            await generator.generate_core_methods()
        elif args.handlers:
            # Generate mock data by executing handler methods
            await generator.generate_handlers_methods()
        elif args.wrappers:
            # Generate mock data by executing wrapper functions
            await generator.generate_wrappers_methods()
        elif args.models:
            # Generate mock data by instantiating models
            await generator.generate_models_methods()
        elif args.search:
            # Generate mock data by executing search service methods
            await generator.generate_search_methods()
        elif args.endpoints:
            # Generate mock data for all endpoints
            await generator.generate_all_endpoints()
        elif args.all:
            # Generate all mock data
            await generator.generate_all()
        else:
            # Default: generate search methods (most commonly used)
            await generator.generate_search_methods()

        return 0
    except Exception as e:
        print(f"\n❌ Error generating mock data: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
