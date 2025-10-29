import polars as pl
from openai import OpenAI
import os
import json
import re
import time
import traceback
from ratelimit import limits, sleep_and_retry
from dotenv import load_dotenv

# API configuration
base_url = "https://chat-ai.academiccloud.de/v1"
model = "openai-gpt-oss-120b"
load_dotenv()  # load .env file into environment
api_key = os.environ["API_KEY"]

PERIOD_IN_SECONDS = 60
MAX_CALLS_PER_PERIOD = 15
STEP_SIZE = 100
MAX_BATCH_ATTEMPTS = 10
MAX_ROW_ATTEMPTS = 5
DEBUG = True
REASONING_LOG_FILE = f"scripts/translate_reasoning_output.txt"

client = OpenAI(api_key=api_key, base_url=base_url)

# ------------------------------------------------------------------------------------------------------


@sleep_and_retry
@limits(calls=MAX_CALLS_PER_PERIOD, period=PERIOD_IN_SECONDS)
def get_translation(user_prompt: str, system_prompt: str):
    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": system_prompt,
            },
            {"role": "user", "content": user_prompt},
        ],
        model=model,
        temperature=0,
    )
    return chat_completion.model_dump()


# ------------------------------------------------------------------------------------------------------


def translate_batch(to_translate: pl.DataFrame, start: int, end: int, system_prompt: str):
    originals = []
    translations = []

    with open(REASONING_LOG_FILE, "a", encoding="utf-8") as f:
        for index in range(start, end):
            dump = get_translation(to_translate.item(index, "Lde"), system_prompt)
            translations.append(dump["choices"][0]["message"]["content"])

            # entire response for debug purposes
            #with open(f"{index}.json", "w") as f:
                #json.dump(dump, f, ensure_ascii=False, indent=2)

            # just the message to more easily understand the "thinking process"
            f.write(dump["choices"][0]["message"]["reasoning_content"])
            f.write('\n\n')

            if DEBUG:
                print(f"row {index} done", flush=True)

    batch_output = pl.Series("translation", translations)

    print(f"batch of rows {start} to {end} done!", flush=True)

    return batch_output


# ------------------------------------------------------------------------------------------------------


def translate(to_translate: pl.DataFrame, system_prompt: str) -> pl.DataFrame:
    batch_outputs = pl.Series(name="Len", dtype=pl.String)
    start = 0

    if os.path.exists(REASONING_LOG_FILE):
        os.remove(REASONING_LOG_FILE)

    while start < to_translate.height:
        end = (
            start + STEP_SIZE
            if start + STEP_SIZE < to_translate.height
            else to_translate.height
        )
        attempt = 0
        done = False

        while not done and attempt < MAX_BATCH_ATTEMPTS:
            try:
                attempt += 1
                if attempt > 1:
                    print(
                        f"attempt #{attempt} for batch of rows {start} to {end}",
                        flush=True,
                    )

                batch_output = translate_batch(to_translate, start, end, system_prompt)
                batch_outputs.append(batch_output)
                done = True

            except Exception as e:
                if DEBUG:
                    print(traceback.format_exc())

        start = end

    print("all batches done")

    return to_translate.with_columns(Len = batch_outputs)