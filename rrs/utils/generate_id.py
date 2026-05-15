import hashlib


def get_consistent_hash(my_pk):
    # Convert the object to a string representation
    obj_str = str(my_pk)
    sha256 = hashlib.sha256()
    # Update the hash object with the object's string representation
    sha256.update(obj_str.encode('utf-8'))
    hash_value = sha256.hexdigest()

    return hash_value