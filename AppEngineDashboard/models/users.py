from google.appengine.ext import ndb
# Import of the algorithm used for hashing.
from hashlib import sha256
from base64 import b64encode
from os import urandom
import uuid

class Users(ndb.Model):
    email = ndb.StringProperty(required=True)
    password = ndb.StringProperty(required=True)
    name = ndb.StringProperty(required=True)
    role = ndb.StringProperty(required=True)
    business = ndb.StringProperty(required=False)
    sensors = ndb.StringProperty(required=False)
    confirmation_code = ndb.StringProperty(required=True)
    confirmed_email = ndb.BooleanProperty(default=False)

    # Whenever we create a function in a class, we need to wrap it around a 'classmethod'
    @classmethod
    def check_if_exists(cls, email):
        # We're querying for any entity in the Users class that contains an email equal to the email we're passing in.
        # The get will return an instance if it finds one. If not, it'll return a 'None'.
        return cls.query(cls.email == email).get()

    @classmethod
    def add_new_user(cls, email, password, name, role, business, sensors):
        user = cls.check_if_exists(email)

        if not user:
            # Create the salt.
            random_bytes = urandom(64)
            salt = b64encode(random_bytes).decode('utf-8')
            # Salt concatenated hashed string of salt concatenated with password will be our hashed password.
            # Reason is that we need to store the salt. Since the salt is fixed length,
            # we can slice the hashed pass when retrieving it to get the salt and the password out of it.
            hashed_password = salt + sha256(salt + password).hexdigest()

            confirmation_code = str(uuid.uuid4().get_hex())


            if business is None:
                # A key consists of a class (=Users) and a unique ID.
                new_user_key = cls(
                    email=email,
                    password=hashed_password,
                    name=name,
                    role=role,
                    confirmation_code=confirmation_code
                ).put()
            else:
                # A key consists of a class (=Users) and a unique ID.
                new_user_key = cls(
                    email=email,
                    password=hashed_password,
                    name=name,
                    role=role,
                    business=business,
                    sensors=sensors,
                    confirmation_code=confirmation_code
                ).put()

            print new_user_key

            return {
                'created': True,
                'user_id': new_user_key.id(),    # We don't want the whole key, only the ID.
                'confirmation_code': confirmation_code
            }

        else:
            return {
                'created': False,
                'title': 'This email is already in use',
                'message': 'Please log in if this is your email account'
            }

    @classmethod
    def update_user(cls, name, email, user_id):
        user = cls.check_if_exists(email=email)
        if user:
            user.name = name
            print user
            user.put()

        # user_key = Users.get_by_id(user)
        # print user_key
        # user_key.name = name
        # print user_key
        # user_key.put()
        return {
            'updated': True,
        }

    @classmethod
    def check_password(cls, email, password):
        # Get the user by email.
        user = cls.check_if_exists(email)
        if user:
            hashed_password = user.password
            salt = hashed_password[:88]  # Slice the hashed password to extract the salt from it. We don't know the exact salt, but we do know how long the salt is

            check_password = salt + sha256(salt + password).hexdigest()

            if check_password == hashed_password:
                if user.confirmed_email:
                    return user.key.id()
                else:
                    return 1
            else:
                return None
        else:
            return None

    @classmethod
    def check_confirmation(cls, user_id, confirmation_code):
        user_id = int(user_id)
        confirmation_code = confirmation_code
        user_key = Users.get_by_id(user_id)

        if user_key.confirmation_code == confirmation_code:
            cls.update_user_confirmed(user_id=user_id)
            return user_key
        else:
            return None

    @classmethod
    def update_user_confirmed(cls, user_id):
        user_key = Users.get_by_id(user_id)
        user_key.confirmed_email = True
        user_key.put()