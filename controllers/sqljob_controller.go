/*
Copyright 2022.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/mmontes11/mariadb-operator/api/v1alpha1"
	mariadbv1alpha1 "github.com/mmontes11/mariadb-operator/api/v1alpha1"
	"github.com/mmontes11/mariadb-operator/pkg/builder"
	"github.com/mmontes11/mariadb-operator/pkg/conditions"
	"github.com/mmontes11/mariadb-operator/pkg/refresolver"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SqlJobReconciler reconciles a SqlJob object
type SqlJobReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	Builder           *builder.Builder
	RefResolver       *refresolver.RefResolver
	ConditionComplete *conditions.Complete
}

//+kubebuilder:rbac:groups=mariadb.mmontes.io,resources=sqljobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=mariadb.mmontes.io,resources=sqljobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=mariadb.mmontes.io,resources=sqljobs/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;patch
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=list;watch;create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *SqlJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var sqlJob mariadbv1alpha1.SqlJob
	if err := r.Get(ctx, req.NamespacedName, &sqlJob); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	ok, result, err := r.waitForDependencies(ctx, &sqlJob)
	if !ok {
		return result, fmt.Errorf("error waiting for dependencies: %v", err)
	}

	mariaDb, err := r.RefResolver.MariaDB(ctx, &sqlJob.Spec.MariaDBRef, sqlJob.Namespace)
	if err != nil {
		var mariaDbErr *multierror.Error
		mariaDbErr = multierror.Append(mariaDbErr, err)

		err = r.patchStatus(ctx, &sqlJob, r.ConditionComplete.RefResolverPatcher(err, mariaDb))
		mariaDbErr = multierror.Append(mariaDbErr, err)

		return ctrl.Result{}, fmt.Errorf("error getting MariaDB: %v", mariaDbErr)
	}

	return ctrl.Result{}, nil
}

func (r *SqlJobReconciler) waitForDependencies(ctx context.Context, sqlJob *v1alpha1.SqlJob) (bool, ctrl.Result, error) {
	if sqlJob.Spec.DependsOn == nil {
		return true, ctrl.Result{}, nil
	}
	for _, dep := range sqlJob.Spec.DependsOn {
		sqlJobDep, err := r.RefResolver.SqlJob(ctx, &dep, sqlJob.Namespace)
		if err != nil {
			return false, ctrl.Result{}, fmt.Errorf("error getting SqlJob dependency: %v", err)
		}
		if !sqlJobDep.IsReady() {
			return false, ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}
	}
	return true, ctrl.Result{}, nil
}

func (r *SqlJobReconciler) patchStatus(ctx context.Context, sqlJob *mariadbv1alpha1.SqlJob,
	patcher conditions.Patcher) error {
	patch := client.MergeFrom(sqlJob.DeepCopy())
	patcher(&sqlJob.Status)

	if err := r.Client.Status().Patch(ctx, sqlJob, patch); err != nil {
		return fmt.Errorf("error patching SqlJob status: %v", err)
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *SqlJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mariadbv1alpha1.SqlJob{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
