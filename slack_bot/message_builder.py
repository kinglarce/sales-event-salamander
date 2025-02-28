from typing import Dict, List

class SlackMessageBuilder:
    def build_ticket_count_message(self, counts: Dict[str, int], schema: str) -> List[dict]:
        """Builds a message block for ticket counts."""
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"Ticket Counts for {schema.upper()}"
                }
            }
        ]
        
        for category, count in counts.items():
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{category.capitalize()}*: {count}"
                }
            })
        
        return blocks 