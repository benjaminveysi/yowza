import re
with open("cloud/manager.go", "r") as f:
    content = f.read()

# Remove the m.db.Compact() call from Cloud Manager since it's now handled internally inside FetchWAL
fixed_manager = """	// Automatic Compaction Trigger
	if len(entries) >= 10 { // Arbitrary small threshold for demo
		log.Println("[Cloud Manager] WAL threshold reached. Triggering cloud snapshot compaction...")
		if err := m.CompactSnapshot(); err != nil {
			log.Printf("[Cloud Manager] Warning: Cloud Snapshot compaction failed: %v", err)
		}
	}"""

content = re.sub(r'\t// Automatic Compaction Trigger\n\tif len\(entries\) >= 10 \{.*?\}\n\t\}', fixed_manager, content, flags=re.DOTALL)

with open("cloud/manager.go", "w") as f:
    f.write(content)
