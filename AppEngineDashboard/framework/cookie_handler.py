from hashlib import sha256

# Secret is stored on the server, which no one has access to.
# It is the same for all cookies.
SECRET = "kzcdGz5NfJKei80S4Yw8lBc4/NUiC8p1Mj9KGZdBtwXEl5WGMsF0q+LGRZjGchg9u8a/ngRB1Z9p90ATkP8RWg=="

# Value is the raw value of what we want to sign. e.g. the user_id
def sign_cookie(value):
    string_value = str(value)
    signature = sha256(SECRET + string_value).hexdigest()
    # Separate signature and value with a pipe character to easily slice the string.
    return signature + "|" + string_value

def check_cookie(value):
    # Slice the value up until pipe character to extract the signature.
    signature = value[:value.find('|')]
    declared_value = value[value.find('|') + 1:]

    # The signature should match the hashed value of the secret message concatenated with the declared value.
    if sha256(SECRET + declared_value).hexdigest() == signature:
        return declared_value
    else:
        # Here we know that the cookie doesn't match and we will not log the user in.
        # Which could mean the cookie has been faked.
        return None
