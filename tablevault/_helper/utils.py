import uuid


def gen_tv_id()-> str:
    return str(uuid.uuid4())

def gen_lock_id(process_id:str)->str:
    return 're-' + process_id + '_' + str(uuid.uuid4())
