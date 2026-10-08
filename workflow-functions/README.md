# Spot-Project-Workflows
Different serverless workflows for the spot project

## Workflows

1. Chain workflow:
   
    In this workflow, each step will run linearly and output from one step will be forwarded to other and so on. Something on the lines of 1->2->3->4

    Implemented on the master node .
    
    To run, `curl http://172.22.85.220:31314/wf1`

2. Branch workflow:
   
   In this workflow, 3 steps will run linearly and then it will branch off to two parallel workflows .

   To run, `curl http://172.22.85.220:31314/bwf1`

