from flask import Flask,request

from database.db import hide_flat


def create_app():
    app = Flask(__name__)

    @app.route('/')
    def index():
        return "Hello, World!"

    @app.route('/update')
    def update_db():
        # update_database()  # Function to update the database
        item_id = request.args.get('id')  # Returns None if 'id' is not present
        result = ''
        if item_id:
            result = hide_flat(item_id)
        return f"hello {item_id} {result}"

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=4000)