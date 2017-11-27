

class DDBMS:
    def __init__(self,inputfile):
        self.inputf = open(inputfile)
        self._run()

    def _run(self):
        print('Start')
        for line in self.inputf:
            line = line.split(')')[0]
            line = line.split('(')
            method = line[0]
            args = line[1].split(',')
            getattr(self,method)(*args)
        print('Done')

    def begin(self,trx):
        print('{} begins'.format(trx))

    def beginRO(self,trx):
        print('RO {} begins'.format(trx))
    
    def R(self,trx,var):
        print('Read {} for {}'.format(var,trx))

    def W(self,trx,var,val):
        print('Write {} to {} for {}'.format(var,val,trx))
    
    def dump(self,arg):
        if not arg:
            print('Dump all')
        elif 'x' in arg:
            print('Dump value of {}'.format(arg))
        else:
            print('Dump values of all variables in site {}'.format(arg))
    
    def end(self,trx):
        print('{} ends'.format(trx))

    def fail(self,site):
        print('Site {} fails'.format(site))

    def recover(self,site):
        print('Recover site {}'.format(site))

if __name__ == '__main__':
    ddbms = DDBMS('input')