"""Settings come from environment variables so that no secret is ever written to a file or to the state."""
import os


def env(name, default=""):
    return os.environ.get(name, default).strip()


class Settings:
    def __init__(self):
        self.state_uri = env("STATE_URI", "./data/state.json")     # a local path, or gs://bucket/object.json
        self.app_token = env("APP_TOKEN")                           # when set, every API call must send it
        self.environment = env("ENVIRONMENT", "dev")
        self.schema_prefix = env("SCHEMA_PREFIX")                   # e.g. "mcp_" gives mcp_bronze, mcp_silver, mcp_gold
        self.scheduler = env("SCHEDULER", "on") != "off"
        self.retry_base_seconds = float(env("RETRY_BASE_SECONDS", "5"))
        self.gcp_project = env("GCP_PROJECT") or env("GOOGLE_CLOUD_PROJECT")
        # alert channels
        self.chat_webhook = env("ALERT_WEBHOOK_URL")
        self.pager_webhook = env("PAGER_WEBHOOK_URL")
        self.smtp_host = env("SMTP_HOST")
        self.smtp_port = int(env("SMTP_PORT", "587") or 587)
        self.smtp_user = env("SMTP_USER")
        self.smtp_password = env("SMTP_PASSWORD")
        self.smtp_from = env("SMTP_FROM")
        self.alert_email_to = env("ALERT_EMAIL_TO")
        # agent
        self.anthropic_api_key = env("ANTHROPIC_API_KEY")
        self.agent_model = env("AGENT_MODEL", "claude-sonnet-5-5")

    def channels(self):
        """Which alert channels can actually deliver."""
        return {
            "email": bool(self.smtp_host and self.alert_email_to),
            "chat": bool(self.chat_webhook),
            "pager": bool(self.pager_webhook),
        }

    def public(self):
        return {
            "env": self.environment,
            "prefix": self.schema_prefix,
            "channels": self.channels(),
            "agent": bool(self.anthropic_api_key),
            "agentModel": self.agent_model,
            "protected": bool(self.app_token),
            "scheduler": self.scheduler,
        }


settings = Settings()
