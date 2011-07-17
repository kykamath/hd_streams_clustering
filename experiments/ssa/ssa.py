'''
Created on Jul 17, 2011

@author: kykamath
'''
from library.mrjobwrapper import CJSONProtocol
from ssa_agg_mr import SSAAggrigationMR
from ssa_sim_mr import SSASimilarityMR

stream_in_multiple_clusters = 'stream_in_multiple_clusters'
iteration_file = '/tmp/ssa_iteration'


def createFileForNextIteration(data):
    with open(iteration_file, 'w') as fp: [fp.write(CJSONProtocol.write(k, v)+'\n') for k, v in data.iteritems()]
    

class StreamSimilarityAggregationMR:
    @staticmethod
    def estimate(fileName, args='-r inline'.split(), **kwargs):
        similarityJob = SSASimilarityMR(args=args)
        dataFromIteration = dict(list(similarityJob.runJob(inputFileList=[fileName], **kwargs)))
        while stream_in_multiple_clusters in dataFromIteration:
            del dataFromIteration[stream_in_multiple_clusters]
            createFileForNextIteration(dataFromIteration)
            aggrigationJob = SSAAggrigationMR(args=args)
            dataFromIteration = dict(list(aggrigationJob.runJob(inputFileList=[iteration_file], **kwargs)))
        for id in dataFromIteration: yield [id]+dataFromIteration[id]['s']              
