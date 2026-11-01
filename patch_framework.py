import re

with open("framework/framework.go", "r") as f:
    content = f.read()

# Make GenericUpdate extensible
new_generic = """// GenericUpdate represents a normalized incoming message from any messenger or service.
type GenericUpdate struct {
	UpdateID string `json:"update_id"`
	UserID   string `json:"user_id"`
	Text     string `json:"text"`
	// Additional fields for dynamic payloads
	RawPayload []byte
	Provider   string
}"""

content = re.sub(r'type GenericUpdate struct \{.*?\n\}', new_generic, content, flags=re.DOTALL)

with open("framework/framework.go", "w") as f:
    f.write(content)
