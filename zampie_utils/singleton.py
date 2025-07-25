class Singleton(type):
    """
    description:
    param:
    return:
    """

    def __init__(self, *args, **kwargs):
        """
        description:
        param:
        return:
        """

        self.__instance = None
        super().__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        """
        description:
        param:
        return:
        """
        if self.__instance is None:
            self.__instance = super().__call__(*args, **kwargs)
        return self.__instance
