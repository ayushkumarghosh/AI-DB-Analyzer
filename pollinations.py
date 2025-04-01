from openai import OpenAI
import json
import tiktoken

MODEL = "openai-large"
MAX_TOKENS = 16384
MAX_CONTEXT_TOKENS = 15000
TEMPERATURE = 0

class StructuredChat:
    def __init__(self, json_schema, model=MODEL, max_tokens=MAX_TOKENS, max_context_tokens=MAX_CONTEXT_TOKENS):
        self.client = OpenAI(base_url="https://text.pollinations.ai/openai", api_key="sample_text")
        self.json_schema = json_schema
        self.model = model
        self.max_tokens = max_tokens
        self.max_context_tokens = max_context_tokens  # Total token limit (e.g., 4096 for GPT-3.5-turbo)
        self.context_budget = int(max_context_tokens * 0.8)  # 80% for input, leaving room for output
        self.encoding = tiktoken.encoding_for_model(model_name="gpt-4o")  # For token counting
        
        self.history = [
            {
                "role": "system",
                "content": (
                    "You are a helpful assistant that provides structured responses in JSON format. "
                    "Your response must strictly adhere to the following JSON schema:\n"
                    f"{json.dumps(json_schema, indent=2)}\n"
                    "Return only the JSON object in your response, nothing else."
                )
            }
        ]
        self.tool = {
            "type": "function",
            "function": {
                "name": "generate_json",
                "description": "Generate a JSON object that matches the provided schema",
                "parameters": json_schema
            }
        }

    def _count_tokens(self, messages):
        """Count total tokens in a list of messages."""
        total_tokens = 0
        for msg in messages:
            total_tokens += len(self.encoding.encode(msg["content"]))
        # Add approximate tokens for role and structure (about 4 per message)
        total_tokens += len(messages) * 4
        return total_tokens

    def _truncate_history(self):
        """Truncate history to fit within context budget, keeping system message."""
        if len(self.history) <= 1:  # Only system message
            return
        
        current_tokens = self._count_tokens(self.history)
        if current_tokens <= self.context_budget:
            return
        
        # Keep system message (index 0) and remove oldest user/assistant messages
        system_message = self.history[0]
        temp_history = [system_message]
        for msg in reversed(self.history[1:]):  # Start from newest
            temp_tokens = self._count_tokens(temp_history + [msg])
            if temp_tokens <= self.context_budget:
                temp_history.append(msg)
            else:
                break
        
        self.history = [system_message] + list(reversed(temp_history[1:]))

    def send_message(self, user_message):
        self.history.append({"role": "user", "content": user_message})
        self._truncate_history()  # Ensure history fits within context window
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=self.history,
            tools=[self.tool],
            tool_choice={"type": "function", "function": {"name": "generate_json"}},
            max_tokens=self.max_tokens,
            temperature=TEMPERATURE
        )
        
        assistant_message = response.choices[0].message
        
        if assistant_message.tool_calls:
            tool_call = assistant_message.tool_calls[0]
            json_response = json.loads(tool_call.function.arguments)
            history_content = json.dumps(json_response, indent=2)
        else:
            history_content = assistant_message.content or "No response"
            json_response = None
        
        self.history.append({"role": "assistant", "content": history_content})
        self._truncate_history()  # Ensure updated history still fits
        
        return json_response if json_response else {"error": "No structured response provided", "content": history_content}

# if __name__ == "__main__":
#     review_schema = {
#         "type": "object",
#         "properties": {
#             "product_name": {"type": "string"},
#             "rating": {"type": "integer", "minimum": 1, "maximum": 5},
#             "review_text": {"type": "string"},
#             "review_date": {"type": "string", "format": "date"},
#             "would_recommend": {"type": "boolean"}
#         },
#         "required": ["product_name", "rating", "review_text", "review_date", "would_recommend"]
#     }
    
#     chat = StructuredChat(review_schema)
    
#     # Test with multiple messages
#     print("Response 1:", chat.send_message("Write a review for a wireless mouse you recently purchased."))
#     print("Response 2:", chat.send_message("Change the rating to 2."))
#     print("Response 3:", chat.send_message("Change the comment to a negative one."))