from utils.http_wrapper import HttpWrapper


class DiscordWrapper(object):
    def webhook_send_message_to_channel(self, webhook_url, message):
        return HttpWrapper.post(webhook_url, {'content': message})
