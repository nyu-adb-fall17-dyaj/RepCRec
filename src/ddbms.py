'''Distributed database management system

A simulator of distributed database management system

Authors:
    Da Ying (dy877@nyu.edu)
    Ardi Jusufi (aj2223@nyu.edu)
'''

from .ticker import Ticker
from .variable import Variable
from .dbsite import DBSite
from .transaction_manager import TransactionManager
from .util import Util

import argparse


class DDBMS:
    """Parses input transactions and dispatches them according to the operation."""

    def __init__(self, inputfile=None):
        """
        Initializes the input (command-line or from a file), all sites and the transaction manager.
        :param inputfile: filepath to input file, if any
        """
        self.inputf = None
        self.cmd = False
        if inputfile is None:
            self.parser = argparse.ArgumentParser(
                description="Run Replicated Concurrency Control and Recovery database.")
            self.init_arguments()
        else:
            self.inputf = open(inputfile)
        self.sites = {}
        self.init_site()
        self.tm = TransactionManager(self.sites)
        self.run()
        self.querystate()

    def init_arguments(self):
        """
        Parses runtime arguments for reading command-line input or from a filepath.
        """
        self.parser.add_argument('--cmd', action='store_true',
                                 help="Specify flag if you wish to enter input via command line.")
        self.parser.add_argument('-file', help="Filepath to input file.")

        args = self.parser.parse_args()
        self.cmd = args.cmd
        if(self.cmd == False):
            inputfile = 'input'  # default input file if filepath not specified
            if(args.file is not None):
                inputfile = args.file
            self.inputf = open(inputfile)

    def init_site(self):
        """
        Initializes ten sites indexed from 1 - 10.
        """
        for i in range(1, 11):
            self.sites[i] = DBSite(i)

    def querystate(self):
        """
        Prints the state of sites and transactions.
        """
        print('----------System State at Time {}'.format(Ticker.get_tick()))
        for s in self.sites:
            self.sites[s].querystate()
        print('~~~~~~~~~~Transactions~~~~~~~~~~')
        self.tm.querystate()

    def run(self):
        """
        Reads the input line by line and calls the respective operation.
        """
        print('Start')
        if(self.cmd == True):  # run interactively via command line
            line = None
            while line != '':
                line = input()
                self._parse_line(line)
        else:  # parse input file
            for line in self.inputf:
                self._parse_line(line)
        print('Done')

    def _parse_line(self, line):
        """
        Parses each line from the input.
        :param line: line to be parsed, e.g. R(T1,x1)
        """
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
        getattr(self, method)(*args)  # call respective method
        Ticker.next_tick()

    def begin(self, trx):
        """
        Begins read/write transaction.

        :param trx: read/write transaction
        """
        print('{} begins'.format(trx))
        self.tm.begin(trx)

    def beginRO(self, trx):
        """
        Begins read-only transaction.

        :param trx: read-only transaction
        """
        print('RO {} begins'.format(trx))
        self.tm.beginRO(trx)

    def R(self, trx, var):
        """
        Dispatches read transaction trx with variable var.

        :param trx: read transaction
        :param var: variable being read
        """
        self.tm.read(trx, var)

    def W(self, trx, var, val):
        """
        Dispatches write transaction with value val on variable var.

        :param trx: write transaction
        :param var: variable being written on
        :param val: value being written on var
        """
        self.tm.write(trx, var, int(val))

    def dump(self, arg):
        """
        Dispatches dump transactions to variables, sites, or displays a system-wide state.

        :param arg: variable for dump(variable); site for dump(site); or None for system-wide dump
        """
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
        """
        Dispatches dump requests to display states of all sites.
        """
        for s in self.sites:
            self.sites[s].dump()

    def _dump_var(self, var):
        """
        Dispatches dump requests to variable var.

        :param var: variable whose state is to be requested
        """
        sites = self._locate_var(var)
        for s in sites:
            self.sites[s].dump_var(var)

    def _dump_site(self, site):
        """
        Dispatches dump requests to site.

        :param site: site whose state is to be requested
        """
        self.sites[int(site)].dump()

    def _locate_var(self, var):
        """
        Returns a list of sites where a variable is stored. If the variable is even, it is stored on all sites.
        If odd, it is stored on site with index 1 + (index % 10), e.g. x3 is stored on site 1 + (3 % 10) = 4.
        :param var: Variable whose site location is being requested
        :return: list of sites where the variable is stored
        """
        var_id = int(var[1:])
        if var_id % 2 == 0:
            return self.sites.keys()
        else:
            return [1 + (var_id % 10)]

    def detect_and_resolve_cycles(self):
        """
        Detects cycles in wait-for graph. If there's a cycle in the graph, it aborts
        the youngest transaction (with the latest timestamp).
        """
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
        """
        Dispatches end transaction trx and checks if there's any deadlock.
        :param trx: transaction to be ended
        """
        # Check if there is any deadlock to be resolved before committing.
        # However, only check if there was no prior deadlock checking
        # at the beginning of this tick:
        if(not Ticker.get_tick() % 5 == 0):
            self.detect_and_resolve_cycles()
        print('{} ends'.format(trx))
        self.tm.end(trx)

    def fail(self, site):
        """
        Causes site to fail.
        :param site: failing site
        """
        print('Site {} fails'.format(site))
        self.sites[int(site)].fail()

    def recover(self, site):
        """
        Causes site to recover.
        :param site: recovering site
        """
        print('Recover site {}'.format(site))
        self.sites[int(site)].recover()
        self.tm.retry_transaction()


if __name__ == '__main__':
    ddbms = DDBMS()
