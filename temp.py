##!/usr/bin/env python
##import numpy as np
##import matplotlib.pyplot as plt
##
### example data
##x = np.arange(0.1, 4, 0.5)
##y = np.exp(-x)
##
### example variable error bar values
##yerr = 0.1 + 0.2*np.sqrt(x)
##xerr = 0.1 + yerr
##
##print x
##print y
##print yerr
##print xerr
##
### First illustrate basic pyplot interface, using defaults where possible.
##plt.figure()
##plt.errorbar(x, y, marker='-', xerr=0.2, yerr=0.4)
##plt.title("Simplest errorbars, 0.2 in x, 0.4 in y")
##
##plt.show()
#
### Now switch to a more OO interface to exercise more features.
##fig, axs = plt.subplots(nrows=2, ncols=2, sharex=True)
##ax = axs[0,0]
##ax.errorbar(x, y, yerr=yerr, fmt='o')
##ax.set_title('Vert. symmetric')
##
### With 4 subplots, reduce the number of axis ticks to avoid crowding.
##ax.locator_params(nbins=4)
##
##ax = axs[0,1]
##ax.errorbar(x, y, xerr=xerr, fmt='o')
##ax.set_title('Hor. symmetric')
##
##ax = axs[1,0]
##ax.errorbar(x, y, yerr=[yerr, 2*yerr], xerr=[xerr, 2*xerr], fmt='--o')
##ax.set_title('H, V asymmetric')
##
##ax = axs[1,1]
##ax.set_yscale('log')
### Here we have to be careful to keep all y values positive:
##ylower = np.maximum(1e-2, y - yerr)
##yerr_lower = y - ylower
##
##ax.errorbar(x, y, yerr=[yerr_lower, 2*yerr], xerr=xerr,
##                            fmt='o', ecolor='g')
##ax.set_title('Mixed sym., log y')
##
##fig.suptitle('Variable errorbars')
##
##plt.show()
#from multiprocessing import Queue, Pool
#
#def innerMeth(arg):
#    print queue.get(), arg, d[arg]
#    
#class C:
#    @staticmethod
#    def meth():
#        queue=Queue()
#        [queue.put('id:%s'%i) for i in xrange(10)]
##        def it(): yield queue.get()
#        
#        d = dict([(u,u) for u in range(10)])
#        pool = Pool()
#        pool.map(innerMeth, xrange(10))
#
#C.meth()
from library.math_modified import getLargestPrimeLesserThan

for dimensions in range(10**4,21*10**4,10**4): print dimensions, getLargestPrimeLesserThan(dimensions)