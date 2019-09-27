__all__ = ['DBOperation']


class DBOperation:

    @classmethod
    def query(cls, table, session):
        try:
            result = session.query(table).all()
            return result
        except Exception as e:
            print(e)
            return None
