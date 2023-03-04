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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SqlJobSpec defines the desired state of SqlJob
type SqlJobSpec struct {
	ConnectionSpec `json:",inline"`

	DependsOn []corev1.LocalObjectReference `json:"dependsOn,omitempty" webhook:"inmutable"`

	Sql                *string                      `json:"sql,omitempty" webhook:"inmutable"`
	SqlConfigMapKeyRef *corev1.ConfigMapKeySelector `json:"sqlConfigMapKeyRef,omitempty" webhook:"inmutableinit"`
}

// SqlJobStatus defines the observed state of SqlJob
type SqlJobStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

func (s *SqlJobStatus) SetCondition(condition metav1.Condition) {
	if s.Conditions == nil {
		s.Conditions = make([]metav1.Condition, 0)
	}
	meta.SetStatusCondition(&s.Conditions, condition)
}

//+kubebuilder:object:root=true
//+kubebuilder:resource:shortName=smdb
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].status"
//+kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.conditions[?(@.type==\"Ready\")].message"
//+kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// SqlJob is the Schema for the sqljobs API
type SqlJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SqlJobSpec   `json:"spec,omitempty"`
	Status SqlJobStatus `json:"status,omitempty"`
}

func (s *SqlJob) IsReady() bool {
	return meta.IsStatusConditionTrue(s.Status.Conditions, ConditionTypeReady)
}

//+kubebuilder:object:root=true

// SqlJobList contains a list of SqlJob
type SqlJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SqlJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SqlJob{}, &SqlJobList{})
}
