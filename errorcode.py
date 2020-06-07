#!/usr/bin/python3

class Errorcode(object):

    errorCode = None 
    errorDescription = None
    errorDate = None
    errorTime = None
    errorAlert = None
    errorOccurences = None
    errorWarning = None

    def __init__(self, dictionary):
        self.__dict__.update(dictionary)

    def __str__(self):
        return f"Error code: {self.errorCode} \n"\
            f"Alert: {self.errorAlert}"