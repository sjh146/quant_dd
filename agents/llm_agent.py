import os
import json
import logging
from typing import Dict, Any, Optional

# llm library of your choice, here using a generic requests/REST based approach 
# or assuming `openai` could be used. We'll build a modular interface.
try:
    import openai
except ImportError:
    openai = None

logger = logging.getLogger(__name__)

class NewsLLMAgent:
    def __init__(self, provider: str = "openai", api_key: Optional[str] = None):
        """
        Initialize the LLM Agent for analyzing financial news.
        :param provider: 'openai', 'local' (e.g., vLLM, Ollama), etc.
        """
        self.provider = provider
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        
        if self.provider == "openai" and openai:
            openai.api_key = self.api_key

    def _build_prompt(self, news_text: str, related_stock: str) -> str:
        return f"""
You are a quantitative financial analyst agent.
Analyze the following news snippet and evaluate its impact on the stock: {related_stock}.

News:
"{news_text}"

Return ONLY a valid JSON dictionary with the following schema:
{{
    "sentiment_score": float (-1.0 to 1.0, where 1.0 is highly positive),
    "theme_keyword": string (e.g., "AI", "Semiconductor", "Biotech"),
    "impact_duration": string ("short", "medium", "long"),
    "confidence": float (0.0 to 1.0)
}}
"""

    def analyze_news(self, news_text: str, related_stock: str) -> Dict[str, Any]:
        """
        Passes the news to the LLM and returns a structured theme/sentiment analysis.
        """
        prompt = self._build_prompt(news_text, related_stock)
        
        try:
            if self.provider == "openai" and openai:
                # Example using OpenAI API
                response = openai.ChatCompletion.create(
                    model="gpt-4",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0
                )
                raw_result = response.choices[0].message.content
                return json.loads(raw_result)
            elif self.provider == "mock":
                # For testing without burning API credits
                logger.info("Using MOCK LLM response")
                return {
                    "sentiment_score": 0.8,
                    "theme_keyword": "Semiconductor",
                    "impact_duration": "short",
                    "confidence": 0.85
                }
            else:
                logger.error(f"Unsupported LLM provider or missing library: {self.provider}")
                return {}
                
        except json.JSONDecodeError:
            logger.error("LLM failed to return valid JSON.")
            return {}
        except Exception as e:
            logger.error(f"LLM API Error: {e}")
            return {}
