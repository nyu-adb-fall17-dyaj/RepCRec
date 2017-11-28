class Ticker:
    tick=0

    @staticmethod
    def next_tick():
        Ticker.tick+=1

    @staticmethod
    def get_tick():
        return Ticker.tick
