__all__ = ['fullname']


def fullname(script_id: str, script_name: str) -> str:
    """
    Concat script_id and script_name to a full name string of a script.
    """
    return f'{script_id}_{script_name}'
