## Service Chargeback in Amazon ECS

This example code provided with the blog, provides a solution to track all tasks that run in a service's lifetime and then associate a cost for the ECS service (considering every task's resource reservations and the amount of time the task ran).

## License Summary

This sample code is made available under a modified MIT license. See the LICENSE file.


1) Deploy lambda_function.py to AWS Lambda
2) Configure Cloudwatch Events
3) Run either ecs_charge_back_flask as a Flask Service or Use ecs-chargeback.py


## Database Structure

Run the following queries;

CREATE DATABASE ecs_task_tracker;

CREATE TABLE `task_definitions` (
  `taskArn` varchar(500) NOT NULL,
  `clusterArn` varchar(500) DEFAULT NULL,
  `containerInstanceArn` varchar(500) DEFAULT NULL,
  `cpu` int(11) DEFAULT NULL,
  `group` varchar(45) DEFAULT NULL,
  `groupName` varchar(200) DEFAULT NULL,
  `instanceId` varchar(45) DEFAULT NULL,
  `instanceType` varchar(45) DEFAULT NULL,
  `launchType` varchar(45) DEFAULT NULL,
  `memory` varchar(45) DEFAULT NULL,
  `osType` varchar(45) DEFAULT NULL,
  `region` varchar(45) DEFAULT NULL,
  `runTime` int(11) DEFAULT NULL,
  `startedAt` varchar(200) DEFAULT NULL,
  `stoppedAt` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`taskArn`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
