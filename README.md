# MapReduce

TODO:

master

1//
- ping worker for state on connection
  - if idle give job


2//
- worker ping master when state idle( after doing job )

worker
- on `do_job` 
  - set state to busy
  - send_after 1sec to do the job