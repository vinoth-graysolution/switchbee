import asyncio
import os
from dotenv import load_dotenv
from websockets.asyncio.client import connect as websocket_connect
import aiohttp

load_dotenv()

async def test_websocket():
    url = "wss://api.sarvam.ai/text-to-speech/ws?model=bulbul:v2"
    api_key = os.getenv("SARVAM_API_KEY")
    headers = {
        "api-subscription-key": api_key,
    }
    print(f"Connecting to WebSocket: {url} with key: {api_key[:10]}...")
    try:
        async with websocket_connect(url, additional_headers=headers) as ws:
            print("Successfully connected to WebSocket!")
            # Send config
            config = {
                "type": "config",
                "data": {
                    "target_language_code": "en-IN",
                    "speaker": "anushka",
                    "speech_sample_rate": "8000",
                    "enable_preprocessing": False,
                    "model": "bulbul:v2"
                }
            }
            import json
            await ws.send(json.dumps(config))
            print("Sent config, waiting for response or closing...")
            await asyncio.sleep(2)
    except Exception as e:
        print(f"WebSocket Connection Failed: {e}")

async def test_http():
    url = "https://api.sarvam.ai/text-to-speech"
    api_key = os.getenv("SARVAM_API_KEY")
    headers = {
        "api-subscription-key": api_key,
        "Content-Type": "application/json"
    }
    payload = {
        "text": "Hello, how can I help you today?",
        "target_language_code": "en-IN",
        "speaker": "anushka",
        "sample_rate": 8000,
        "model": "bulbul:v2"
    }
    print(f"Sending POST request to HTTP endpoint: {url}...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
                print(f"HTTP Status: {resp.status}")
                if resp.status == 200:
                    data = await resp.json()
                    print(f"HTTP Success! Received audios count: {len(data.get('audios', []))}")
                else:
                    text = await resp.text()
                    print(f"HTTP Error: {text}")
    except Exception as e:
        print(f"HTTP Request Failed: {e}")

async def main():
    await test_websocket()
    print("-" * 40)
    await test_http()

if __name__ == "__main__":
    asyncio.run(main())
