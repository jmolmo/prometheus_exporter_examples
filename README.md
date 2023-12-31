# prometheus_exporter_examples

My collection of prometheus exporters for different tasks:

## pv_metadata_exporter.py

Generate a new metric for each image with "image metadata set".
If the metadata present in the image is like:

    > rbd image-meta list exampleRBD/exampleIMG --format json
    {"PV":"testPV","PVC":"testPVC","namespace":"testNameSpace"}

Then the exporter will generate these metrics:

    > curl http://localhost:8089
    ...
    # HELP ceph_rbd_image_labels rbd image labels coming from metadata
    # TYPE ceph_rbd_image_labels gauge
    ceph_rbd_image_labels{image="exampleIMG",namespace="testNameSpace",pool="exampleRBD",pv="testPV",pvc="testPVC"} 1.0
    # HELP ceph_rbd_image_labels_collect_seconds time taken to gather rbd image labels from ceph (secs)
    # TYPE ceph_rbd_image_labels_collect_seconds gauge
    ceph_rbd_image_labels_collect_seconds 0.5086851119995117

After that, the new metrics can be combined with other image rbd metrics, example:

    ceph_rbd_read_bytes{image="exampleIMG", instance="192.168.122.7:9283", job="ceph", pool="exampleRBD"} = 123
    and
    ceph_rbd_image_labels{image="exampleIMG", instance="192.168.122.145:8089", job="custom_labels", namespace="testNameSpace", pool="exampleRBD", pv="testPV", pvc="testPVC"}=1.0

And the combination will be:

    ceph_rbd_read_bytes * on(image, pool) group_left(namespace, pvc, pv) ceph_rbd_image_labels
    {image="exampleIMG", instance="192.168.122.7:9283", job="ceph", namespace="testNameSpace", pool="exampleRBD", pv="testPV", pvc="testPVC"}=123

