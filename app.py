

# version 4 

from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit, join_room, leave_room
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import os
import psycopg2

app = Flask(__name__)
app.config['SECRET_KEY'] = '9912277968'
socketio = SocketIO(app, cors_allowed_origins="*")

# MongoDB connection
client = MongoClient('mongodb://nearme:RMxDDDGW1xcwtiJOhH4r3fhVHtOQxY3c0v1O4YZueYSMPBissaEAtVjIThlJEBGpBFTqxYSLhonEACDbiJ1eRQ==@nearme.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@nearme@')
db = client['sssv1']
conversations = db['chat']
users = db['users']

@app.route('/api/conversations', methods=['POST'])
def create_conversation():
    data = request.json
    conversation = {
        'conversation_id': str(ObjectId()),
        'participants': data['participants'],
        'messages': [],
        'created_at': datetime.utcnow(),
        'updated_at': datetime.utcnow()
    }
    result = conversations.insert_one(conversation)
    return jsonify({'conversation_id': conversation['conversation_id']}), 201

@app.route('/api/conversations/<conversation_id>', methods=['GET'])
def get_conversation(conversation_id):
    conversation = conversations.find_one({'conversation_id': conversation_id})
    if conversation:
        conversation['_id'] = str(conversation['_id'])
        return jsonify(conversation)
    return jsonify({'error': 'Conversation not found'}), 404


@app.route('/api/conversations/<conversation_id>/messages', methods=['GET'])
def get_messages(conversation_id):
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('pageSize', 20))
    since = request.args.get('since', '0')

    skip = (page - 1) * page_size
    query = {'conversation_id': conversation_id}

    if since != '0':
        query['messages.timestamp'] = {'$gt': since}

    conversation = conversations.find_one(query, {'messages': {'$slice': [skip, page_size]}})

    if conversation:
        messages = conversation.get('messages', [])
        return jsonify(messages)
    return jsonify([]), 404  # Return 404 if conversation not found

@app.route('/api/users', methods=['POST'])
def create_user():
    data = request.json
    user = {
        'username': data['username'],
        'profile_image': data.get('profile_image', '')
    }
    result = users.insert_one(user)
    return jsonify({'user_id': str(result.inserted_id)}), 201


# @app.route('/api/conversationslist/<user_id>', methods=['GET'])
# def get_user_conversations(user_id):
#     try:
#         # Query for conversations where the user_id is in the participants list
#         conversations_cursor = conversations.find({
#             'participants': user_id
#         })
        
#         conversation_list = list(conversations_cursor)
        
#         if not conversation_list:
#             return jsonify({"error": "No conversations found"}), 404

#         response = []
#         for conv in conversation_list:
#             # Sort messages by timestamp in descending order and get the first one
#             sorted_messages = sorted(conv['messages'], key=lambda x: x['timestamp'], reverse=True)
#             last_message = sorted_messages[0]['message'] if sorted_messages else ''
            
#             conversation_data = {
#                 'conversation_id': str(conv['conversation_id']),
#                 'participants': conv['participants'],
#                 'last_message': last_message,
#                 'updated_at': conv['updated_at']
#             }
#             response.append(conversation_data)
        
#         # Sort the response by updated_at in descending order
#         response = sorted(response, key=lambda x: x['updated_at'], reverse=True)
        
#         return jsonify(response), 200
#     except Exception as e:
#         return jsonify({'error': str(e)}), 500

def execute_query(query, params=None):
    print("called execute_query")
    db_config = {
        'host': 'database-1-instance-1.c7iqok4sw8cg.ap-south-1.rds.amazonaws.com',
        'port': '5432',
        'database': 'postgres',
        'user': 'postgres',
        'password': 'Nearme1137'
    }
    try:
        print("try")
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()

        # Execute the query with optional parameters
        cursor.execute(query, params)
        if any(keyword in query.strip().upper() for keyword in ["INSERT", "UPDATE", "DELETE"]):
            # For INSERT queries, commit the transaction and return None
            connection.commit()
            row_count = cursor.rowcount
            print(f"Rows affected: {row_count}")
            cursor.close()
            connection.close()
            return row_count
        
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        print(column_names)
        cursor.close()
        connection.close()
        result = [dict(zip(column_names, row)) for row in rows]

        return result
    except Exception as e:
        raise e


@app.route('/api/conversationslist/<user_id>', methods=['GET'])
def get_user_conversations(user_id):
    try:
        # Query for conversations where the user_id is in the participants list
        conversations_cursor = conversations.find({
            'participants': user_id
        })
        
        conversation_list = list(conversations_cursor)
        print(conversation_list)
        
        if not conversation_list:
            return jsonify({"error": "No conversations found"}), 404

        response = []
        for conv in conversation_list:
            # Sort messages by timestamp in descending order and get the first one
            sorted_messages = sorted(conv['messages'], key=lambda x: x['timestamp'], reverse=True)
            last_message = sorted_messages[0]['message'] if sorted_messages else ''
            
            participants_info = []
            for participant_id in conv['participants']:
                # Check if the participant is a business or a user
                if participant_id.startswith("BIZ"):
                    # Retrieve business information from the PostgreSQL database
                    business_query = "SELECT business_uid, business_name, profile_image_url FROM business WHERE business_uid = %s"
                    business_info = execute_query(business_query, (participant_id,))
                    if business_info:
                        participants_info.append({
                            'business_id': business_info[0]['business_uid'],
                            'business_name': business_info[0]['business_name'],
                            'business_image': business_info[0]['profile_image_url']
                        })
                if not participant_id.startswith("BIZ"):
                    # Retrieve user information from the user collection in MongoDB
                    user_info = users.find_one({'userid': participant_id})
                    if user_info:
                        participants_info.append({
                            'user_id': user_info['userid'],
                            'user_name': user_info['username'],
                            'user_image': user_info['profile_image_url']
                        })
            
            conversation_data = {
                'conversation_id': str(conv['conversation_id']),
                'participants': participants_info,
                'last_message': last_message,
                'updated_at': conv['updated_at']
            }
            response.append(conversation_data)
        
        # Sort the response by updated_at in descending order
        response = sorted(response, key=lambda x: x['updated_at'], reverse=True)
        
        return jsonify(response), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500




# @app.route('/api/messages', methods=['POST'])
# def send_message():
#     data = request.json
#     conversation_id = data['conversation_id']
    
#     sender_id = data['sender_id']
#     recipient_id = data['recipient_id']
#     # conversation_id = sender_id + recipient_id
    
#     conversation = conversations.find_one({'conversation_id': conversation_id})
    
#     if not conversation:
#         # Create a new conversation if it doesn't exist
#         conversation = {
#             'conversation_id': conversation_id,
#             'participants': [sender_id, recipient_id],
#             'messages': [],
#             'created_at': datetime.utcnow(),
#             'updated_at': datetime.utcnow()
#         }
#         conversations.insert_one(conversation)
    
#     message = {
#         'message_id': str(ObjectId()),
#         'sender_id': sender_id,
#         'message': data['message'],
#         'timestamp': datetime.utcnow().isoformat(),
#         'read': False
#     }
#     result = conversations.update_one(
#         {'conversation_id': conversation_id},
#         {
#             '$push': {'messages': {'$each': [message], '$position': 0}},
#             '$set': {'updated_at': datetime.utcnow()}
#         }
#     )
#     if result.modified_count > 0 or result.upserted_id:
#         socketio.emit('message', message, room=conversation_id)
#         return jsonify(message), 201
#     else:
#         return jsonify({'error': 'Failed to send message'}), 400

@app.route('/api/messages', methods=['POST'])
def send_message():
    data = request.json
    conversation_id = data['conversation_id']
    
    sender_id = data['sender_id']
    recipient_id = data['recipient_id']

    conversation = conversations.find_one({'conversation_id': conversation_id})
    
    if not conversation:
        # Create a new conversation if it doesn't exist
        conversation = {
            'conversation_id': conversation_id,
            'participants': [sender_id, recipient_id],
            'messages': [],
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        }
        conversations.insert_one(conversation)
    
    message = {
        'message_id': str(ObjectId()),
        'sender_id': sender_id,
        'message': data['message'],
        'timestamp': datetime.utcnow().isoformat(),
        'read': False
    }
    result = conversations.update_one(
        {'conversation_id': conversation_id},
        {
            '$push': {'messages': {'$each': [message], '$position': 0}},
            '$set': {'updated_at': datetime.utcnow()}
        }
    )
    if result.modified_count > 0 or result.upserted_id:
        socketio.emit('message', message, room=conversation_id)
        return jsonify(message), 201
    else:
        return jsonify({'error': 'Failed to send message'}), 400


@socketio.on('join')
def on_join(data):
    username = data['username']
    room = data['room']
    join_room(room)
    emit('user_joined', {'username': username}, room=room)

@socketio.on('leave')
def on_leave(data):
    username = data['username']
    room = data['room']
    leave_room(room)
    emit('user_left', {'username': username}, room=room)

@socketio.on('message')
def handle_message(data):
    message = {
        'message_id': str(ObjectId()),
        'sender_id': data['sender_id'],
        'message': data['message'],
        'timestamp': datetime.utcnow().isoformat(),
        'read': False
    }
    conversations.update_one(
        {'conversation_id': data['conversation_id']},
        {
            '$push': {'messages': {'$each': [message], '$position': 0}},
            '$set': {'updated_at': datetime.utcnow()}
        }
    )
    emit('message', message, room=data['conversation_id'])

if __name__ == '__main__':
    socketio.run(app, debug=True)