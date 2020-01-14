package nifi

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	nifiv1alpha1 "github.com/sburges/nifi-operator/pkg/apis/nifi/v1alpha1"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_nifi")

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new NiFi Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileNiFi{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("nifi-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource NiFi
	err = c.Watch(&source.Kind{Type: &nifiv1alpha1.NiFi{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner NiFi
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &nifiv1alpha1.NiFi{},
	})

	err = c.Watch(&source.Kind{Type: &appsv1.StatefulSet{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &nifiv1alpha1.NiFi{},
	})

	err = c.Watch(&source.Kind{Type: &corev1.Service{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &nifiv1alpha1.NiFi{},
	})

	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileNiFi implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileNiFi{}

// ReconcileNiFi reconciles a NiFi object
type ReconcileNiFi struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
}

// Reconcile reads that state of the cluster for a NiFi object and makes changes based on the state read
// and what is in the NiFi.Spec
func (r *ReconcileNiFi) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling NiFi")

	// Fetch the NiFi instance
	instance := &nifiv1alpha1.NiFi{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	//ensure headless service is healthy
	result, err := r.reconcileHS(instance, instance.Name+"-hs", reqLogger)
	if err != nil {
		return result, err
	}

	//ensure LB is healthy
	result, err = r.reconcileLB(instance, instance.Name+"-lb", reqLogger)
	if err != nil {
		return result, err
	}

	//ensure stateful set is healhty
	result, err = r.reconcileSS(instance, instance.Name, reqLogger)
	if err != nil {
		return result, err
	}

	return reconcile.Result{}, nil
}

func (r *ReconcileNiFi) reconcileHS(cr *nifiv1alpha1.NiFi, name string, logger logr.Logger) (reconcile.Result, error) {
	found := &corev1.Service{}
	err := r.client.Get(context.TODO(), types.NamespacedName{Name: name, Namespace: cr.Namespace}, found)
	logger.Info("Looking for existing Service", "Service.Name", name)

	if err != nil && errors.IsNotFound(err) {
		hs := r.newHS(cr, name)
		return r.create(hs, logger)
	} else if err != nil {
		logger.Error(err, "Failed to get Service")
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

func (r *ReconcileNiFi) newHS(cr *nifiv1alpha1.NiFi, name string) *corev1.Service {
	ser := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cr.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Name: "nifi-listen-http", Port: 8081},
				{Name: "nifi-site-protocol", Port: 2881},
				{Name: "nifi-node-protocol", Port: 2882},
			},
			Selector:  map[string]string{"app": cr.Name},
			ClusterIP: "None",
		},
	}
	controllerutil.SetControllerReference(cr, ser, r.scheme)
	return ser
}

func (r *ReconcileNiFi) reconcileLB(cr *nifiv1alpha1.NiFi, name string, logger logr.Logger) (reconcile.Result, error) {
	found := &corev1.Service{}
	err := r.client.Get(context.TODO(), types.NamespacedName{Name: name, Namespace: cr.Namespace}, found)
	logger.Info("Looking for existing Service", "Service.Name", name)

	if err != nil && errors.IsNotFound(err) {
		hs := r.newLB(cr, name)
		return r.create(hs, logger)
	} else if err != nil {
		logger.Error(err, "Failed to get Service")
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

func (r *ReconcileNiFi) newLB(cr *nifiv1alpha1.NiFi, name string) *corev1.Service {
	ser := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cr.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Name: "nifi-ui", Port: 8080, TargetPort: intstr.FromInt(8080)},
			},
			Selector: map[string]string{"app": cr.Name},
			Type:     corev1.ServiceTypeLoadBalancer,
		},
	}
	controllerutil.SetControllerReference(cr, ser, r.scheme)
	return ser
}

func (r *ReconcileNiFi) reconcileSS(cr *nifiv1alpha1.NiFi, name string, logger logr.Logger) (reconcile.Result, error) {
	ss := &appsv1.StatefulSet{}
	err := r.client.Get(context.TODO(), types.NamespacedName{Name: name, Namespace: cr.Namespace}, ss)
	logger.Info("Looking for existing statefulset", "SS.Name", name)

	if err != nil && errors.IsNotFound(err) {
		hs := r.newSS(cr)
		return r.create(hs, logger)
	} else if err != nil {
		logger.Error(err, "Failed to get statefulset")
		return reconcile.Result{}, err
	}

	result, err := r.reconcileReplicaCount(cr, *ss, logger)
	if err != nil {
		return result, err
	}

	logger.Info("Reconciling missing nodes", "SS.Name", name)
	result, err = r.removeMissing(cr, *ss, logger)
	if err != nil {
		return result, err
	}

	logger.Info("Reconciling Disconnected Nodes", "SS.Name", name)
	return r.restartDisconnected(cr, *ss, logger)
}

func (r *ReconcileNiFi) reconcileReplicaCount(cr *nifiv1alpha1.NiFi, ss appsv1.StatefulSet, logger logr.Logger) (reconcile.Result, error) {
	// Ensure the deployment size is the same as the spec
	targetSize := cr.Spec.Size
	currentSize := *ss.Spec.Replicas
	logger.Info("Verifying number of replicas", "Target Replicas", targetSize, "Actual Replicas", currentSize)

	if currentSize != targetSize {
		logger.Info("Update Node Count", "Target Replicas", targetSize, "Actual Replicas", currentSize)

		ss.Spec.Replicas = &targetSize
		err := r.client.Update(context.TODO(), &ss)
		if err != nil {
			logger.Error(err, "Failed to update statefulset", "Statefulset.Name", ss.Name)
			return reconcile.Result{}, err
		}
		// Spec updated - return and requeue
		return reconcile.Result{Requeue: true}, nil
	}

	// Spec updated - return and requeue
	return reconcile.Result{}, nil
}

func (r *ReconcileNiFi) removeMissing(cr *nifiv1alpha1.NiFi, ss appsv1.StatefulSet, logger logr.Logger) (reconcile.Result, error) {
	client := NewClient(cr.Name + "-lb")
	if client.Healthy() == false {
		logger.Info("NiFi Cluster is not ready yet")
		return reconcile.Result{}, nil
	}

	//remove nodes from cluster
	podList, err := r.getPodList(ss.Namespace, cr.Name)
	if err != nil {
		logger.Error(err, "Error getting list of pods")
	}

	err = client.DeleteMissing(podList)
	if err != nil {
		logger.Error(err, "Error querying missing nodes")
	}

	return reconcile.Result{}, nil
}

func (r *ReconcileNiFi) restartDisconnected(cr *nifiv1alpha1.NiFi, ss appsv1.StatefulSet, logger logr.Logger) (reconcile.Result, error) {
	client := NewClient(cr.Name + "-lb")
	if client.Healthy() == false {
		logger.Info("NiFi Cluster is not ready yet")
		return reconcile.Result{}, nil
	}

	dn, err := client.findDisconnected()
	if err != nil {
		logger.Error(err, "Error finding disconnected nodes")
	}

	podList, err := r.getPods(ss.Namespace, cr.Name)

	for _, node := range dn {
		for _, pod := range podList.Items {
			if node == pod.Name {
				if pod.CreationTimestamp.Before(&metav1.Time{Time: time.Now().Add(time.Minute * -10)}) {
					logger.Info("Restarting disconnected node", "Pod Name", pod.Name)
					err := r.client.Delete(context.TODO(), &pod)
					if err != nil {
						logger.Error(err, "Failed to restart disconnected node")
					}
				}
			}
		}
	}

	return reconcile.Result{}, nil
}

func (r *ReconcileNiFi) newSS(cr *nifiv1alpha1.NiFi) *appsv1.StatefulSet {
	replicas := cr.Spec.Size

	ls := labelsForNiFi(cr.Name)

	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name,
			Namespace: cr.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: ls,
			},
			ServiceName: cr.Name + "-hs",
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ls,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Image: cr.Spec.NiFiImage,
						Name:  cr.Name,
						Ports: []corev1.ContainerPort{{
							ContainerPort: 8080,
							Name:          "http",
						}},
						Env: []corev1.EnvVar{
							{
								Name:  "NIFI_CLUSTER_IS_NODE",
								Value: "true",
							},
							{
								Name:  "NIFI_ZK_CONNECT_STRING",
								Value: cr.Spec.ZKConnect,
							},
							{
								Name:  "ZK_MONITOR_PORT",
								Value: "2888",
							},
							{
								Name:  "NIFI_CLUSTER_NODE_PROTOCOL_PORT",
								Value: "8081",
							},
						},
					}},
				},
			},
		},
	}

	controllerutil.SetControllerReference(cr, ss, r.scheme)

	return ss
}

func labelsForNiFi(name string) map[string]string {
	return map[string]string{"app": name}
}

func (r *ReconcileNiFi) create(object runtime.Object, logger logr.Logger) (reconcile.Result, error) {
	logger.Info("Creating a new resource", "Type:", object.GetObjectKind())
	err := r.client.Create(context.TODO(), object)
	if err != nil {
		logger.Error(err, "Failed to create resource", "Type:", object.GetObjectKind().GroupVersionKind().Kind)
		return reconcile.Result{}, err
	}
	// Service created successfully - return and requeue
	return reconcile.Result{Requeue: true}, nil
}

func (r *ReconcileNiFi) getPods(namespace string, name string) (corev1.PodList, error) {
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels(labelsForNiFi(name)),
	}

	if err := r.client.List(context.TODO(), podList, listOpts...); err != nil {
		logger.Error(err, "Failed to list pods", "NiFi", name)
	}

	return *podList, nil
}

func (r *ReconcileNiFi) getPodList(namespace string, name string) ([]string, error) {
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels(labelsForNiFi(name)),
	}

	if err := r.client.List(context.TODO(), podList, listOpts...); err != nil {
		logger.Error(err, "Failed to list pods", "NiFi", name)
		return nil, err
	}

	podNames := getPodNames(podList.Items)

	return podNames, nil
}

// getPodNames returns the pod names of the array of pods passed in
func getPodNames(pods []corev1.Pod) []string {
	var podNames []string
	for _, pod := range pods {
		podNames = append(podNames, pod.Name)
	}
	return podNames
}
