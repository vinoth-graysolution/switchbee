#
# Copyright (c) 2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import os

from dotenv import load_dotenv
from loguru import logger

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import TextFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.runner.types import RunnerArguments
from pipecat.runner.utils import parse_telephony_websocket
from pipecat.serializers.exotel import ExotelFrameSerializer
from pipecat.services.sarvam import SarvamTTSService
from pipecat.services.deepgram.stt import DeepgramSTTService
# from pipecat.services.elevenlabs.tts import ElevenLabsTTSService
from pipecat.services.openai.llm import OpenAILLMService

from pipecat.transcriptions.language import Language
from pipecat.transports.base_transport import BaseTransport
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)

from prompt import get_system_prompt

load_dotenv(override=True)


async def run_bot(transport: BaseTransport, handle_sigint: bool):

    logger.info("Initializing AI voice pipeline")

    # ----------------- LLM ----------------- #

    llm = OpenAILLMService(
        api_key=os.getenv("OPENAI_API_KEY"),
    )

    # ----------------- STT ----------------- #

    stt = DeepgramSTTService(
        api_key=os.getenv("DEEPGRAM_API_KEY"),
        language=Language.EN,
    )

    # ----------------- TTS ----------------- #

    tts = SarvamTTSService(
        api_key=os.getenv("SARVAM_API_KEY"),
        model="bulbul:v3-beta",
        voice_id="shubh",
        params=SarvamTTSService.InputParams(
            language=Language.EN,
            pace=1.1,
            temperature=0.01,
        ),
    )

    # ----------------- CONTEXT ----------------- #

    messages = [
        {
            "role": "system",
            "content": get_system_prompt(),
        }
    ]

    context = LLMContext(messages=messages)

    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(
            vad_analyzer=SileroVADAnalyzer(),
        ),
    )

    # ----------------- PIPELINE ----------------- #

    pipeline = Pipeline(
        [
            transport.input(),
            stt,
            user_aggregator,
            llm,
            tts,
            transport.output(),
            assistant_aggregator,
        ]
    )

    # ----------------- TASK ----------------- #

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            audio_in_sample_rate=8000,
            audio_out_sample_rate=8000,
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
    )

    # ----------------- EVENTS ----------------- #

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        logger.info("Client connected - starting conversation")

        # Initial greeting from bot
        await task.queue_frames(
            [
                TextFrame(
                    "Hello, am I speaking with the candidate?"
                )
            ]
        )

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        logger.info("Client disconnected")
        await task.cancel()

    # ----------------- RUNNER ----------------- #

    runner = PipelineRunner(handle_sigint=handle_sigint)

    await runner.run(task)


async def bot(runner_args: RunnerArguments):
    """Main bot entry point."""

    logger.info("Waiting for Exotel websocket connection")

    import json

    # Read first two messages to find the 'start' event
    stream_sid = ""
    call_sid = ""
    
    for _ in range(2):
        try:
            msg_raw = await runner_args.websocket.receive_text()
            msg = json.loads(msg_raw)
            logger.info(f"Received WS message: {msg}")
            if msg.get("event") == "start" and "start" in msg:
                start_data = msg["start"]
                stream_sid = start_data.get("stream_sid") or start_data.get("streamSid", "")
                call_sid = start_data.get("call_sid") or start_data.get("callSid", "")
                break
        except Exception as e:
            logger.error(f"Error reading initial WS message: {e}")
            break

    if not stream_sid:
        logger.warning("Could not extract stream_sid from initial messages")

    serializer = ExotelFrameSerializer(
        stream_sid=stream_sid,
        call_sid=call_sid,
    )

    transport = FastAPIWebsocketTransport(
        websocket=runner_args.websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            add_wav_header=False,
            serializer=serializer,
        ),
    )

    await run_bot(
        transport=transport,
        handle_sigint=runner_args.handle_sigint,
    )