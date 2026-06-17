package k8s

type RoomRangeCalc struct {
	ns         string
	svcName    string
	maxRoom    int
	podOrdinal int // 当前pod序号 statefulset game-0,game-1
	totalPod   int // statefulset副本数
}

func NewRoomRangeCalc() (*RoomRangeCalc, error) {
	// 获取当前pod hostname game-0
	//host, _ := os.Hostname()
	//ordinalStr := strings.Split(host, "-")[1]
	//ordinal, _ := strconv.Atoi(ordinalStr)
	//
	//// 加载kubeconfig
	//kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")
	//config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	//if err != nil {
	//	return nil, err
	//}
	//clientset, err := kubernetes.NewForConfig(config)
	//if err != nil {
	//	return nil, err
	//}
	//sts, err := clientset.AppsV1().StatefulSets(namespace).Get(context.Background(), svcName, metav1.GetOptions{})
	//if err != nil {
	//	return nil, err
	//}
	//replicas := int(*sts.Spec.Replicas)
	//
	//return &RoomRangeCalc{
	//	ns:         namespace,
	//	svcName:    svcName,
	//	maxRoom:    maxRoom,
	//	podOrdinal: ordinal,
	//	totalPod:   replicas,
	//}, nil
	return nil, nil
}

// GetCurrentPodRoomRange 返回当前pod负责的房间区间 [start, end]
func (r *RoomRangeCalc) GetCurrentPodRoomRange() (uint64, uint64) {
	perPod := uint64(r.maxRoom / r.totalPod)
	start := uint64(r.podOrdinal) * perPod
	end := start + perPod - 1
	return start, end
}

// IsRoomBelong 判断roomId是否属于当前pod
func (r *RoomRangeCalc) IsRoomBelong(roomId uint64) bool {
	return true
	//s, e := r.GetCurrentPodRoomRange()
	//return roomId >= s && roomId <= e
}
