from ticker import Ticker
from variable import Variable
from dbsite import DBSite
from transaction_manager import TransactionManager
from util import Util

import argparse

class DDBMS:
    def __init__(self):
        self.inputf = None
        self.cmd = False
        self.parser = argparse.ArgumentParser(description="Run Replicated Concurrency Control and Recovery database.")
        self.init_arguments()
        self.sites = {}
        self.init_site()
        self.tm = TransactionManager(self.sites)
        self.run()
        self.querystate()

    def init_arguments(self):
        self.parser.add_argument('--cmd', action='store_true', help="Specify flag if you wish to enter input via command line.")
        self.parser.add_argument('-file', help="Filepath to input file.")

        args = self.parser.parse_args()
        self.cmd = args.cmd
        if(self.cmd == False):
            inputfile = 'input'  # default input file if filepath not specified
            if(args.file is not None):
                inputfile = args.file
            self.inputf = open(inputfile)

    def init_site(self):
        for i in range(1, 11):
            self.sites[i] = DBSite(i)

    def querystate(self):
        print('----------System State at Time {}'.format(Ticker.get_tick()))
        for s in self.sites:
            self.sites[s].querystate()
        print('~~~~~~~~~~Transactions~~~~~~~~~~')
        self.tm.querystate()

    def run(self):
        print('Start')
        if(self.cmd == True): # run interactively via command line
            line = None
            while line != '':
                line = input()
                self._parse_line(line)
        else: # parse input file
            for line in self.inputf:
                self._parse_line(line)
        print('Done')

    def _parse_line(self, line):
        if not line:
            return
        tick = Ticker.get_tick()
        line = line.split(')')[0]
        line = line.split('(')
        method = line[0]
        args = [x.strip() for x in line[1].split(',')]

        print('----------Tick {}----------'.format(tick))
        # Detect cycles every five ticks:
        if (tick % 5 == 0):
            self.detect_and_resolve_cycles()
        getattr(self, method)(*args)
        Ticker.next_tick()

    def begin(self, trx):
        print('{} begins'.format(trx))
        self.tm.begin(trx)

    def beginRO(self, trx):
        print('RO {} begins'.format(trx))
        self.tm.beginRO(trx)

    def R(self, trx, var):
        self.tm.read(trx, var)

    def W(self, trx, var, val):
        self.tm.write(trx, var, int(val))

    def dump(self, arg):
        if not arg:
            print('Dump all')
            self._dump_all()
        elif 'x' in arg:
            print('Dump value of {}'.format(arg))
            self._dump_var(arg)
        else:
            print('Dump values of all variables in site {}'.format(arg))
            self._dump_site(arg)

    def _dump_all(self):
        for s in self.sites:
            self.sites[s].dump()

    def _dump_var(self, var):
        sites = self._locate_var(var)
        for s in sites:
            self.sites[s].dump_var(var)

    def _dump_site(self, site):
        self.sites[int(site)].dump()

    def _locate_var(self, var):
        var_id = int(var[1:])
        if var_id % 2 == 0:
            return self.sites.keys()
        else:
            return [1 + (var_id % 10)]

    def detect_and_resolve_cycles(self):
        waits_for_graph = self.tm._generate_waits_for_graph()
        cycles = Util.get_cycles(waits_for_graph)
        if(len(cycles) > 0):
            # Randomly select the first cycle to resolve:
            cycle = cycles[0]
            print("Detected cycle: ", cycle)

            # Find youngest transaction in cycle:
            latest_timestamp = 0
            youngest_transaction = None
            for trx in cycle:
                t = self.tm.trxs[trx]
                if(t.timestamp > latest_timestamp):
                    latest_timestamp = t.timestamp
                    youngest_transaction = trx
            self.tm.abort(youngest_transaction)
            self.detect_and_resolve_cycles()

    def end(self, trx):
        # Check if there is any deadlock to be resolved before committing.
        # However, only check if there was no prior deadlock checking
        # at the beginning of this tick:
        if(not Ticker.get_tick() % 5 == 0):
            self.detect_and_resolve_cycles()
        print('{} ends'.format(trx))
        self.tm.end(trx)

    def fail(self, site):
        print('Site {} fails'.format(site))
        self.sites[int(site)].fail()

    def recover(self, site):
        print('Recover site {}'.format(site))
        self.sites[int(site)].recover()
        self.tm.retry_transaction()


if __name__ == '__main__':
    ddbms = DDBMS()