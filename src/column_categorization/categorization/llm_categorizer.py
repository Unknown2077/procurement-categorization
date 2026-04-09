from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol

from openai import OpenAI

from column_categorization.schemas.categorization import ValueMapping

SYSTEM_PROMPT = """# High-Fidelity Taxonomy Categorizer

## 1. Role Description
Acts as a specialized Taxonomy Categorizer for technical and structured data. Its primary function is to transform raw, extracted concepts into clean, canonical categories while strictly preserving specification-level differences.

## 2. Core Capabilities
- Preserve technical specification differences as distinct categories.
- Merge only near-duplicates, spelling errors, or exact synonyms.
- Every category must be a concise noun phrase with 1-5 words.

## 3. Constraints
- Never merge items with different technical attributes.
- Prefer specific labels over broad labels.
- Output language must be Indonesian.
"""


@dataclass(frozen=True)
class CategorizationBatchResult:
    column_description: str
    mappings: list[ValueMapping]


class BatchCategorizer(Protocol):
    def categorize_batch(
        self,
        column_name: str,
        column_description: str | None,
        raw_values: list[str],
    ) -> CategorizationBatchResult: ...


class OpenAILLMCategorizer:
    def __init__(
        self,
        api_key: str,
        model: str = "qwen/qwen3-next-80b-a3b-instruct",
        base_url: str | None = None,
        client: OpenAI | None = None,
    ) -> None:
        if client is None:
            if not api_key.strip():
                raise ValueError("NIM_API_KEY must not be empty")
            client_kwargs: dict[str, str] = {"api_key": api_key}
            if base_url is not None and base_url.strip():
                client_kwargs["base_url"] = base_url
            self._client = OpenAI(**client_kwargs)
        else:
            self._client = client
        self._model = model

    def categorize_batch(
        self,
        column_name: str,
        column_description: str | None,
        raw_values: list[str],
    ) -> CategorizationBatchResult:
        if not raw_values:
            raise ValueError("raw_values must not be empty")

        user_prompt = self._build_user_prompt(
            column_name=column_name,
            column_description=column_description,
            raw_values=raw_values,
        )
        raw_output = self._request_completion(user_prompt)
        try:
            return self._parse_json_response(raw_output=raw_output, expected_values=raw_values)
        except ValueError as parse_error:
            if not self._is_label_length_error(parse_error):
                raise

        retry_prompt = self._build_retry_prompt(user_prompt)
        retry_output = self._request_completion(retry_prompt)
        try:
            return self._parse_json_response(raw_output=retry_output, expected_values=raw_values)
        except ValueError as retry_error:
            if self._is_label_length_error(retry_error):
                raise ValueError(
                    "Retry failed: labels must be 1-5 words. "
                    f"Last error: {retry_error}"
                ) from retry_error
            raise

    def _build_user_prompt(self, column_name: str, column_description: str | None, raw_values: list[str]) -> str:
        payload = {
            "column_name": column_name,
            "column_hint": column_description,
            "raw_values": raw_values,
            "required_output_schema": {
                "column_description": "string",
                "mappings": [{"raw_value": "string", "labels": ["string"]}],
            },
        }
        return (
            "Perform categorization for the following column values. "
            "Use Indonesian category names only. "
            "Return valid JSON only without markdown. "
            "Return category values in the existing 'labels' field. "
            "Each category must be a noun phrase with 1-5 words, strictly no exceptions. "
            "Do not output any category longer than 5 words. "
            "If one raw_value contains multiple services, split it into multiple categories. "
            "For combined services joined by 'dan', output each service as a separate category. "
            "Example mapping: raw_value 'Jasa Kebersihan dan Keamanan Gedung Kantor Pusat' "
            "must map to labels ['Jasa Kebersihan', 'Jasa Keamanan']. "
            "Valid category examples: 'Jasa Konsultansi', 'Perangkat Server', 'Lisensi ERP'. "
            "Invalid category example: 'Kebersihan dan Keamanan Gedung Kantor Pusat'. "
            "Ensure each raw_value appears exactly once.\n"
            f"{json.dumps(payload, ensure_ascii=False)}"
        )

    def _build_retry_prompt(self, base_prompt: str) -> str:
        return (
            f"{base_prompt}\n\n"
            "Correction required: your previous output used categories longer than 5 words. "
            "Regenerate all categories so every category has 1-5 words only. "
            "Keep JSON schema unchanged (including 'labels' field) and keep one mapping for every raw_value."
        )

    def _request_completion(self, user_prompt: str) -> str:
        completion = self._client.chat.completions.create(
            model=self._model,
            temperature=0,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
        )
        message_content = completion.choices[0].message.content
        raw_output = message_content if isinstance(message_content, str) else ""
        if not raw_output.strip():
            raise ValueError("LLM returned an empty response")
        return raw_output

    def _is_label_length_error(self, error: ValueError) -> bool:
        return "Label exceeds 5 words" in str(error)

    def _parse_json_response(self, raw_output: str, expected_values: list[str]) -> CategorizationBatchResult:
        try:
            data = json.loads(raw_output)
        except json.JSONDecodeError as error:
            raise ValueError(f"LLM output is not valid JSON: {error}") from error

        if not isinstance(data, dict):
            raise ValueError("LLM output root must be a JSON object")

        column_description = data.get("column_description")
        if not isinstance(column_description, str) or not column_description.strip():
            raise ValueError("LLM output must contain non-empty 'column_description'")

        raw_mappings = data.get("mappings")
        if not isinstance(raw_mappings, list) or not raw_mappings:
            raise ValueError("LLM output must contain non-empty list 'mappings'")

        mappings: list[ValueMapping] = []
        seen_values: set[str] = set()
        for item in raw_mappings:
            if not isinstance(item, dict):
                raise ValueError("Each mapping must be an object")
            mapping = ValueMapping.model_validate(item)
            for label in mapping.labels:
                if len(label.split()) > 5:
                    raise ValueError(f"Label exceeds 5 words: '{label}'")
            if mapping.raw_value in seen_values:
                raise ValueError(f"Duplicate raw_value in mapping: '{mapping.raw_value}'")
            seen_values.add(mapping.raw_value)
            mappings.append(mapping)

        missing_values = sorted(set(expected_values) - seen_values)
        if missing_values:
            raise ValueError(f"LLM output missing raw values: {missing_values}")

        return CategorizationBatchResult(column_description=column_description.strip(), mappings=mappings)
