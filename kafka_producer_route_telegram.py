import telebot
import folium
import json
import io
import pandas as pd

TOKEN = 'YOUR_TELEGRAM_BOT_TOKEN'  # Replace with your Telegram bot token

bot = telebot.TeleBot(TOKEN)


@bot.message_handler(commands=['start'])
def send_welcome(message):
    save_user(message.chat.id)
    bot.reply_to(message, "Welcome to the GeoJSON graph bot! Send any message and I'll generate a Folium graph for you.")


@bot.message_handler(func=lambda message: True)
def handle_message(message):
    user_id = message.chat.id
    message_text = message.text
    generate_graph(user_id, message_text)


def save_user(user_id):
    # TODO: Save the user ID in your system
    print(f"User {user_id} saved in the system.")


def generate_graph(user_id, message_text):
    # TODO: Generate the graph based on the user's ID and message text
    print(f"Generating graph for user {user_id} with message: {message_text}")


if __name__ == "__main__":
    bot.polling()
