#
# Copyright (c) 2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

import os
from typing import Callable, Optional

from dotenv import load_dotenv
from loguru import logger

from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import TextFrame, TranscriptionFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.runner.types import RunnerArguments
from pipecat.serializers.exotel import ExotelFrameSerializer
from pipecat.services.sarvam.tts import SarvamTTSService
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.openai.llm import OpenAILLMService

from pipecat.transcriptions.language import Language
from pipecat.transports.base_transport import BaseTransport
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)

from prompt import get_system_prompt

load_dotenv(override=True)


# ─────────────────────────────────────────────────────────────
# Interest signals
# ─────────────────────────────────────────────────────────────

INTERESTED_SIGNALS = [
    "interested",
    "yes",
    "sure",
    "definitely",
    "absolutely",
    "sounds good",
    "i would like",
    "please proceed",
    "go ahead",
    "open to",
    "keen",
    "looking forward",
    "great opportunity",
    "tell me more",
    "i am interested",
    "i'm interested",
    "okay",
    "ok",
    "proceed",
    "continue",
]

NOT_INTERESTED_SIGNALS = [
    "not interested",
    "no thank you",
    "no thanks",
    "not looking",
    "happy where i am",
    "not available",
    "not right now",
    "not suitable",
    "please remove",
    "do not call",
    "don't call",
    "remove me",
    "not for me",
    "declined",
    "wrong number",
    "don't disturb",
    "stop calling",
]


def detect_interest(text: str) -> Optional[str]:
    """
    Returns 'interested', 'not_interested', or None if inconclusive.
    NOT_INTERESTED checked first to avoid false positives.
    """
    lowered = text.lower()
    for phrase in NOT_INTERESTED_SIGNALS:
        if phrase in lowered:
            return "not_interested"
    for phrase in INTERESTED_SIGNALS:
        if phrase in lowered:
            return "interested"
    return None


# ─────────────────────────────────────────────────────────────
# TranscriptionInterceptor
# A lightweight FrameProcessor that sits in the pipeline and
# reads every TranscriptionFrame to detect candidate interest.
# This is the correct Pipecat way — no invalid transport events.
# ─────────────────────────────────────────────────────────────

class TranscriptionInterceptor(FrameProcessor):
    """Pass-through processor that inspects TranscriptionFrames."""

    def __init__(self, candidate_name: str, on_outcome: Callable[[str, str], None]):
        super().__init__()
        self._candidate_name = candidate_name
        self._on_outcome = on_outcome
        self._fired = False

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        # Only inspect user transcriptions (not bot TTS text)
        if (
            isinstance(frame, TranscriptionFrame)
            and not self._fired
            and self._on_outcome
        ):
            text = frame.text or ""
            outcome = detect_interest(text)
            if outcome:
                self._fired = True
                logger.info(
                    f"[InterestDetector] {self._candidate_name} → {outcome} "
                    f"(utterance: '{text}')"
                )
                self._on_outcome(self._candidate_name, outcome)

        # Always pass frame downstream — we are transparent
        await self.push_frame(frame, direction)


# ─────────────────────────────────────────────────────────────
# Core pipeline
# ─────────────────────────────────────────────────────────────

async def run_bot(
    transport: BaseTransport,
    handle_sigint: bool,
    candidate_name: str = "Candidate",
    on_outcome: Optional[Callable[[str, str], None]] = None,
):
    logger.info("Initializing AI voice pipeline")

    # ── LLM ─────────────────────────────────────────────────
    # gpt-4o-mini: ~3x faster TTFB than gpt-4o, ideal for voice
    llm = OpenAILLMService(
        api_key=os.getenv("OPENAI_API_KEY"),
        model="gpt-4o-mini",
    )

    # ── STT ─────────────────────────────────────────────────
    stt = DeepgramSTTService(
        api_key=os.getenv("DEEPGRAM_API_KEY"),
        language=Language.EN,
    )

    # ── TTS ─────────────────────────────────────────────────
    tts = SarvamTTSService(
        api_key=os.getenv("SARVAM_API_KEY"),
        settings=SarvamTTSService.Settings(
            voice="anushka",
            model="bulbul:v2",
            language=Language.EN,
            pitch=0.1,
            pace=1.2,
            loudness=1.5,
        ),
    )

    # ── Context ──────────────────────────────────────────────
    messages = [
        {
            "role": "system",
            "content": get_system_prompt(candidate_name),
        }
    ]

    context = LLMContext(messages=messages)

    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(
            vad_analyzer=SileroVADAnalyzer(),
        ),
    )

    # ── Interest interceptor (placed right after STT) ────────
    interceptor = None
    if on_outcome:
        interceptor = TranscriptionInterceptor(
            candidate_name=candidate_name,
            on_outcome=on_outcome,
        )

    # ── Pipeline ─────────────────────────────────────────────
    pipeline_stages = [transport.input(), stt]
    if interceptor:
        pipeline_stages.append(interceptor)   # <── intercept transcriptions here
    pipeline_stages += [
        user_aggregator,
        llm,
        tts,
        transport.output(),
        assistant_aggregator,
    ]

    pipeline = Pipeline(pipeline_stages)

    # ── Task ─────────────────────────────────────────────────
    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            audio_in_sample_rate=8000,
            audio_out_sample_rate=8000,
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
    )

    # ── Events ───────────────────────────────────────────────
    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        logger.info("Client connected — sending greeting")
        await task.queue_frames(
            [TextFrame(f"Hello, am I speaking with {candidate_name}?")]
        )

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        logger.info("Client disconnected")
        await task.cancel()

    # ── Runner ───────────────────────────────────────────────
    runner = PipelineRunner(handle_sigint=handle_sigint)
    await runner.run(task)


# ─────────────────────────────────────────────────────────────
# Entry point (called by service.py WebSocket endpoint)
# ─────────────────────────────────────────────────────────────

async def bot(
    runner_args: RunnerArguments,
    candidate_names_map: dict = None,
    latest_name: str = "Candidate",
    on_outcome: Optional[Callable[[str, str], None]] = None,
):
    """Main bot entry point."""
    logger.info("Waiting for Exotel websocket connection")

    import json

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

    candidate_name = latest_name
    if candidate_names_map and call_sid and call_sid in candidate_names_map:
        candidate_name = candidate_names_map[call_sid]

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
        transport,
        getattr(runner_args, "handle_sigint", False),
        candidate_name,
        on_outcome=on_outcome,
    )