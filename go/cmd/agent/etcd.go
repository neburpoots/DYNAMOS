package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Jorrit05/DYNAMOS/pkg/etcd"
	pb "github.com/Jorrit05/DYNAMOS/pkg/proto"
)

func getJobName(user string) (string, error) {
	// /agents/jobs/UVA/jorrit.stutterheim@cloudnation.nl -> jorrit-3141334
	jobNameKey := fmt.Sprintf("%s/%s/%s", etcdJobRootKey, agentConfig.Name, user)
	jobName, err := etcd.GetValueFromEtcd(etcdClient, jobNameKey)
	if err != nil {
		logger.Sugar().Errorf("etcd error: %v", err.Error())
		return "", err
	}
	return jobName, nil
}

func getCompositionRequest(userName string, jobName string) (*pb.CompositionRequest, error) {
	var compositionRequest *pb.CompositionRequest

	key := fmt.Sprintf("%s/%s/%s/%s", etcdJobRootKey, agentConfig.Name, userName, jobName)

	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for {
		jsonVal, err := etcd.GetAndUnmarshalJSON(etcdClient, key, &compositionRequest)
		if err == nil && jsonVal != nil {
			break
		}
		if err != nil {
			lastErr = err
			logger.Sugar().Warnf("Error getting composition request for key: %s, error: %v", key, err)
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(1 * time.Second)
	}

	if compositionRequest == nil {
		if lastErr != nil {
			return nil, fmt.Errorf("error getting composition request for user: %v, jobName: %v: %w", userName, jobName, lastErr)
		}
		return nil, fmt.Errorf("no job found for user: %v, jobName: %v", userName, jobName)
	}
	return compositionRequest, nil
}

func registerUserWithJob(ctx context.Context, compositionRequest *pb.CompositionRequest) (context.Context, error) {
	logger.Debug("Entering registerUserWithJob")

	// // /agents/jobs/UVA/jorrit-3141334 ->  pb.CompositionRequest
	// jobNameKey := fmt.Sprintf("%s/%s/%s", etcdJobRootKey, agentConfig.Name, compositionRequest.JobName)

	// /agents/jobs/UVA/jorrit.stutterheim@cloudnation.nl/jorrit-3141334 -> compositionRequest
	userKey := fmt.Sprintf("%s/%s/%s/%s", etcdJobRootKey, agentConfig.Name, compositionRequest.User.UserName, compositionRequest.JobName)

	// One entry with all related info with the jobName as key
	err := etcd.SaveStructToEtcd(etcdClient, userKey, compositionRequest)
	if err != nil {
		logger.Sugar().Warnf("Error saving struct to etcd: %v", err)
		return ctx, err
	}

	// One entry with the jobName with the userName as key
	// err = etcd.PutValueToEtcd(etcdClient, userKey, compositionRequest.JobName, etcd.WithMaxElapsedTime(time.Second*5))
	// if err != nil {
	// 	logger.Sugar().Warnf("Error saving jobname to etcd: %v", err)
	// 	return err
	// }
	return ctx, nil
}
