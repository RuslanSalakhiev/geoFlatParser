from flask import Flask, request

from config import vm_port
from database.db import hide_flat
from tg_bot.tg import hide_message, add_favorite_tag, remove_favorite_tag


def create_app():
    app = Flask(__name__)

    @app.route('/')
    def index():
        return f"Hello"

    @app.route('/update')
    async def update_db():
        item_id = request.args.get('id')
        message_id = request.args.get('message_id')
        if item_id:
            result = hide_flat(item_id)

            if result == 'ok':
                result_message = f"<span style='color:green; font-size: 30px'>Hidden</span>"
                await hide_message(message_id)
            else:
                result_message = f"<span style='color:red; font-size: 30px'>Error</span>"

        return result_message

    @app.route('/add_favorite')
    async def add_favorite():
        item_id = request.args.get('id')
        message_id = request.args.get('message_id')
        if item_id:
            await add_favorite_tag(message_id)

        result_message = f"<span style='color:green; font-size: 30px'>Saved</span>"
        return result_message

    @app.route('/remove_favorite')
    async def remove_favorite():
        item_id = request.args.get('id')
        message_id = request.args.get('message_id')
        if item_id:
            await remove_favorite_tag(message_id)

        result_message = f"<span style='color:green; font-size: 30px'>Removed</span>"
        return result_message

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=vm_port)
