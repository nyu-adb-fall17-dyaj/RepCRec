'''System-wide Ticker

This module records the tick

Authors:
    Da Ying (dy877@nyu.edu)
    Ardi Jusufi (aj2223@nyu.edu)
'''


class Ticker:
    '''Records and provides the current tick'''
    tick = 0

    @staticmethod
    def next_tick():
        Ticker.tick += 1

    @staticmethod
    def get_tick():
        return Ticker.tick
