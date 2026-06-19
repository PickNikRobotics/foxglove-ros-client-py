from foxglove_ros_client.tf import TFClient


class FakeRos:
    def __init__(self):
        self._id_counter = 0

    @property
    def id_counter(self):
        self._id_counter += 1
        return self._id_counter

    def subscribe_topic(self, *_args):
        pass

    def unsubscribe_topic(self, *_args):
        pass


def test_tf_client_resolves_chained_transform_to_fixed_frame():
    client = TFClient(FakeRos(), fixed_frame="map")
    received = []
    client.subscribe("base_link", received.append)

    client._process_tf_message(
        {
            "transforms": [
                _transform("map", "odom", 1, 0, 0),
                _transform("odom", "base_link", 0, 2, 0),
            ]
        }
    )

    assert received[-1]["translation"] == {"x": 1.0, "y": 2.0, "z": 0.0}
    assert received[-1]["rotation"] == {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}


def test_tf_client_callbacks_immediately_when_transform_is_known():
    client = TFClient(FakeRos(), fixed_frame="map")
    client._process_tf_message({"transforms": [_transform("map", "tool0", 0, 0, 3)]})

    received = []
    client.subscribe("tool0", received.append)
    assert received[-1]["translation"] == {"x": 0.0, "y": 0.0, "z": 3.0}


def test_tf_client_can_walk_edges_in_reverse():
    client = TFClient(FakeRos(), fixed_frame="base_link")
    received = []
    client.subscribe("odom", received.append)

    client._process_tf_message({"transforms": [_transform("odom", "base_link", 0, 2, 0)]})
    assert received[-1]["translation"] == {"x": 0.0, "y": -2.0, "z": 0.0}


def _transform(parent, child, x, y, z):
    return {
        "header": {"frame_id": parent},
        "child_frame_id": child,
        "transform": {
            "translation": {"x": x, "y": y, "z": z},
            "rotation": {"x": 0, "y": 0, "z": 0, "w": 1},
        },
    }
