class TopicWriter {
	constructor(client, topic, msg_type) {
		this.client = client;
		this.topic = topic;
		this.msg_type = msg_type;
		this.ui = null;
		this.dc_id = null;
		this.dc = null;
	}

	send(msg) {
		if (!this.client.pc || this.client.pc.connectionState != "connected")
			return false; //not ready

		if (!this.dc || this.dc.readyState != "open") {
			// if (this.dc)
			//     console.warn('Writer dc not ready for '+topic+': '+(this.dcs[topic] ? 'state='+this.dcs[topic].readyState : 'Not initiated'))
			return false; //not yer ready
		}

		if (!this.msg_writer) {
			this.msg_writer = this.client.getMsgWriter(this.msg_type);
		}

		//console.log('Writing '+msg_type+' into '+topic, this.dcs[topic])
		setTimeout(() => {
			if (
				!this.client.pc ||
				this.client.pc.connectionState != "connected" ||
				!this.dc ||
				this.dc.readyState != "open" ||
				!this.msg_writer
			)
				return;
			let payload = this.msg_writer.writeMessage(msg); //to binary
			this.dc.send(payload);
		}, 0);

		return true;
	}
}

class Subscriber {
	constructor(client, id_source) {
		this.client = client;
		this.id_source = id_source;
		this.msg_type = null;

		this.latest = null;
	}
}

class TopicReader {
	constructor(opts) {
		this.topic = opts.topic;
		this.msg_type = opts.msg_type;
		this.dc_id = opts.dc_id;
		this.dc = opts.dc;
		this.msg_reader = null;
		this.msg_queue = []; //stores early messages until we have msg_type definition
		this.logged = false;
	}

	tryGetMessageReader(client) {
		if (this.msg_reader) return true; // only once

		let msg_type_class = client.findMessageType(
			this.msg_type,
			client.supported_msg_types,
		);
		if (!msg_type_class) {
			console.warn(
				"Msg type def " + this.msg_type + " not yet awailable for " + this.topic,
			);
			return false;
		}

		let Reader = window.Serialization.MessageReader;
		this.msg_reader = new Reader([msg_type_class].concat(client.supported_msg_types));

		this.msg_queue.forEach((raw_msg) => {
			client._onDCMessage(this, raw_msg);
		});
		this.msg_queue = []; //clear

		return true;
	}
}

export function IsImageTopic(t) {
	return (
		t == "sensor_msgs/msg/Image" ||
		t == "sensor_msgs/msg/CompressedImage" ||
		t == "ffmpeg_image_transport_msgs/msg/FFMPEGPacket"
	);
}

export function IsVideoTopic(t) {
	return (
		t == "sensor_msgs/msg/Image" ||
		t == "ffmpeg_image_transport_msgs/msg/FFMPEGPacket"
	);
}

export function IsFastVideoTopic(t) {
	return t == "ffmpeg_image_transport_msgs/msg/FFMPEGPacket";
}

export class PhntmBridgeClient extends EventTarget {
	id_robot = null;
	session = null;
	supported_msg_types = []; // served from Bridge Server

	pc = null;

	discovered_topics = {}; // str topic => { msg_types: str[], subscribed: bool }
	discovered_nodes = {}; // str service => { msg_type: str}
	discovered_services = {}; // str service => { msg_type: str}
	discovered_cameras = {}; // str id => { info: {}}
	discovered_docker_containers = {}; // str id => { info: {}}

	topic_configs = {};
	topic_streams = {}; // topic/cam => id_stream
	media_streams = {}; // id_stream => MediaStream

	socket = null;
	event_calbacks = {};
	topic_config_calbacks = {};

	service_request_callbacks = {};
	service_reply_callbacks = {};

	input_manager = null;
	extrernal_scripts = {};

	constructor(opts) {
		super();

		this.id_robot = opts.id_robot;
		if (!this.id_robot) {
			console.error("Missing opts.id_robot");
			return false;
		}
		this.session = null;

		//defaults to phntm bridge service
		this.socket_url = opts.socket_url;
		this.socket_path =
			opts.socket_path !== undefined ? opts.socket_path : "/app/socket.io/";
		this.socket_auto_connect =
			opts.socket_auto_connect !== undefined ? opts.socket_auto_connect : false;

		this.bridge_files_url = opts.bridge_files_url;
		this.bridge_files_secret = null;

		this.ice_servers_config = [];
		this.force_turn = opts.force_turn;

		this.init_complete = false;
		this.msg_writers = {}; //msg_type => writer
		this.msg_readers = {}; //msg_type => reader
		this.topic_writers = {}; //id => writer
		this.topic_readers = {}; //str topic => TopicReader
		this.subscribers = {}; //id => topic or cam reader
		this.can_change_subscriptions = false;
		this.requested_subscription_change = false;
		this.topic_streams = {};
		this.topic_configs = {}; //id => extra topic config
		this.media_streams = {}; //id_stream => stream
		this.latest = {};

		this.supported_msg_types = [];
		this.ui = null; // ui ref

		let that = this;

		this.socket_auth = {
			id_instance: null, // stored for reconnects when known
		};

		this.socket = io(this.socket_url, {
			path: this.socket_path,
			auth: this.socket_auth,
			autoConnect: this.socket_auto_connect,
			reconnection: false,
			forceNew: true,
			// transport: 'websocket'
		});

		this.socket.on("connect", () => {
			console.log("Socket.io connected with id " + that.socket.id);

			setTimeout(() => {
				that._initRobotConnection();
			}, 0);

			setTimeout(() => {
				that.emit("socket_connect"); // ui reconnects
			}, 0);
		});

		this.on("socket_disconnect", () => {
			console.log("Got robot socket disconect");
			that.robot_socket_online = false;
			that.emit("update");
			that._clearConnection();
		});

		this.socket.on("disconnect", (reason) => {
			console.log("Socket.io disconnected, reason: ", reason);
			// io client disconnect
			that.emit("socket_disconnect"); // ui reconnects
		});

		this.socket.on("error", (err) => {
			console.error("Socket.io error: " + err);
			that.emit("socket_disconnect"); // ui reconnects
		});

		this.socket.on("connect_error", (err) => {
			console.error("Socket.io connect error: " + err);
			that.emit("socket_disconnect"); // ui reconnects
		});

		this.socket.on("instance", (id_instance) => {
			setTimeout(() => {
				if (that.socket_auth.id_instance != id_instance) {
					console.warn("Got new id instance: " + id_instance);
					if (that.pc) {
						console.warn(
							"Removing pc for old instance: " +
								that.socket_auth.id_instance,
						);
						that.pc.close();
						that.pc = null;
					}
					that.socket_auth.id_instance = id_instance;
				}
			}, 0);
		});

		this.socket.on("robot", (robot_data, return_callback) => {
			setTimeout(() => {
				that._processRobotData(robot_data, return_callback);
			}, 0);
		});

		this.socket.on("robot_peers", (data) => {
			if (!data[that.id_robot]) return;
			setTimeout(() => {
				console.log("Got peers data", data[that.id_robot]);
				that.emit("robot_peers", data[that.id_robot]);
			}, 0);
		});

		this.socket.on("peer_service_call", (data) => {
			if (!data[that.id_robot]) return;

			setTimeout(() => {
				console.log("Got peer service call broadcast", data[that.id_robot]);
				that.emit("peer_service_call_broadcast", data[that.id_robot]);
			}, 0);
		});

		this.socket.on("robot:update", (update_data, return_callback) => {
			setTimeout(() => {
				that._processRobotData(update_data, return_callback);
			}, 0);
		});

		this.socket.on("introspection", (state) => {
			console.log("Got introspetion state " + state); // undefined
			that.introspection = state;
			setTimeout(() => {
				that.emit("introspection", that.introspection);
			}, 0);
		});

		this.socket.on("defs", (msg_defs_data) => {
			if (!msg_defs_data[that.id_robot]) return;

			let defs = msg_defs_data[that.id_robot];
			console.log("Received message type defs", defs);

			defs.forEach((def) => {
				if (that.findMessageType(def.name)) return; // don't overwrite
				that.supported_msg_types.push(def);
			});

			Object.values(that.topic_readers).forEach((dc_reader) => {
				if (!dc_reader.msg_reader) {
					dc_reader.tryGetMessageReader(that); // clears the queue of early messages
				}
			});

			that.emit("defs_updated");
		});

		this.socket.on("nodes", (nodes_data) => {
			if (!nodes_data[that.id_robot]) return;

			// console.warn('Raw nodes: ', nodes_data);

			setTimeout(() => {
				that.discovered_nodes = {};
				that.discovered_topics = {};
				that.discovered_services = {}; // rewrites all

				Object.keys(nodes_data[that.id_robot]).forEach((node) => {
					that.discovered_nodes[node] = {
						node: node,
						namespace: nodes_data[that.id_robot][node]["namespace"],
						publishers: {},
						subscribers: {},
						services: {},
						params_editable: false,
					};
					if (nodes_data[that.id_robot][node]["publishers"]) {
						let topics = Object.keys(
							nodes_data[that.id_robot][node]["publishers"],
						);
						topics.forEach((topic) => {
							let msg_type =
								nodes_data[that.id_robot][node]["publishers"][topic][
									"msg_type"
								];
							let qos =
								nodes_data[that.id_robot][node]["publishers"][topic][
									"qos"
								];
							that.discovered_nodes[node].publishers[topic] = {
								msg_type: msg_type,
								is_video: IsImageTopic(msg_type),
								msg_type_supported:
									that.findMessageType(msg_type) != null,
								qos: qos,
							};

							that.discovered_topics[topic] = {
								msg_type: msg_type,
								id: topic,
								is_video: IsImageTopic(msg_type),
								msg_type_supported:
									that.findMessageType(msg_type) != null,
							};
						});
					}
					if (nodes_data[that.id_robot][node]["subscribers"]) {
						let topics = Object.keys(
							nodes_data[that.id_robot][node]["subscribers"],
						);
						topics.forEach((topic) => {
							let msg_type =
								nodes_data[that.id_robot][node]["subscribers"][topic][
									"msg_type"
								];
							let qos =
								nodes_data[that.id_robot][node]["subscribers"][topic][
									"qos"
								];
							that.discovered_nodes[node].subscribers[topic] = {
								msg_type: msg_type,
								is_video: IsImageTopic(msg_type),
								msg_type_supported:
									that.findMessageType(msg_type) != null,
								qos: qos,
							};

							if (!that.discovered_topics[topic]) {
								that.discovered_topics[topic] = {
									msg_type: msg_type,
									id: topic,
									is_video: IsImageTopic(msg_type),
									msg_type_supported:
										that.findMessageType(msg_type) != null,
								};
							}
						});
					}
					if (nodes_data[that.id_robot][node]["services"]) {
						let services = Object.keys(
							nodes_data[that.id_robot][node]["services"],
						);
						services.forEach((service) => {
							let msg_type =
								nodes_data[that.id_robot][node]["services"][service];
							that.discovered_nodes[node].services[service] = {
								service: service,
								msg_type: msg_type,
							};

							if (msg_type == "rcl_interfaces/srv/ListParameters")
								that.discovered_nodes[node]["_srvListParameters"] =
									service;
							if (msg_type == "rcl_interfaces/srv/DescribeParameters")
								that.discovered_nodes[node]["_srvDescribeParameters"] =
									service;
							if (msg_type == "rcl_interfaces/srv/GetParameters")
								that.discovered_nodes[node]["_srvGetParameters"] =
									service;
							if (msg_type == "rcl_interfaces/srv/SetParameters")
								that.discovered_nodes[node]["_srvSetParameters"] =
									service;

							if (
								that.discovered_nodes[node]["_srvListParameters"] &&
								that.discovered_nodes[node]["_srvDescribeParameters"] &&
								that.discovered_nodes[node]["_srvGetParameters"] &&
								that.discovered_nodes[node]["_srvSetParameters"]
							) {
								that.discovered_nodes[node].params_editable = true;
							}

							that.discovered_services[service] = {
								service: service,
								msg_type: msg_type,
							};
						});
					}
				});

				console.log("Got nodes ", that.discovered_nodes);
				that.emit("nodes", that.discovered_nodes);

				console.log("Got topics ", that.discovered_topics);
				that.emit("topics", that.discovered_topics);

				console.log("Got services:", that.discovered_services);
				that.emit("services", that.discovered_services);
			}, 0);
		});

		// this.socket.on('cameras', (cameras_data) => {

		//     if (!cameras_data[this.id_robot])
		//         return;

		//     setTimeout(()=>{
		//         this.discovered_cameras = {};

		//         Object.keys(cameras_data[this.id_robot]).forEach((id_camera) => {

		//             this.discovered_cameras[id_camera] = {
		//                 id: id_camera,
		//                 info: cameras_data[this.id_robot][id_camera],
		//             };
		//         });

		//         console.log('Got Cameras:', this.discovered_cameras);
		//         this.emit('cameras', this.discovered_cameras);
		//     }, 0);
		// });

		this.socket.on("docker", (docker_containers_data) => {
			if (!docker_containers_data[this.id_robot]) return;

			setTimeout(() => {
				this.emit("docker", docker_containers_data[this.id_robot]);
			}, 0);
		});

		window.addEventListener("beforeunload", function (e) {
			let stream_ids = Object.keys(that.media_streams);

			stream_ids.forEach((id_stream) => {
				console.warn(
					"Killing stream " + id_stream,
					that.media_streams[id_stream],
				);
				that.media_streams[id_stream].getTracks().forEach((track) => {
					track.stop();
				});
			});

			if (that.pc) {
				console.warn("Unloading window, disconnecting pc...", that.pc);
				that.pc.close();
				that.pc = null;
			}

			// debugger;
		});

		this.on("peer_connected", () => {
			that.startHeartbeat();
		});

		this.on("peer_disconnected", () => {
			that.stopHeartbeat();
		});

		this.on("peer_conection_changed", () => {
			that.reportConectionState(); // update webrtc state on Bridge Server that sends it to the robot
		});
		// pc = InitPeerConnection(id_robot);
	}

	getMsgWriter(msg_type) {
		if (this.msg_writers[msg_type]) return this.msg_writers[msg_type];

		let Writer = window.Serialization.MessageWriter;
		let msg_class = this.findMessageType(msg_type);
		if (!msg_class) {
			console.warn(
				"Failed creating writer for " +
					msg_type +
					"; message class not found (yet)",
			);
			return false;
		}
		console.log("Creating msg writer for " + msg_type + "; class loaded=", msg_class);
		this.msg_writers[msg_type] = new Writer(
			[msg_class].concat(this.supported_msg_types),
		);
		if (this.msg_writers[msg_type] === undefined) {
			console.error(
				"Failed creating writer for " + msg_type + "; message class was loaded",
				msg_class,
				this.msg_writers[msg_type],
				this.supported_msg_types,
			);
			return false;
		}

		return this.msg_writers[msg_type];
	}

	on(event, cb) {
		if (!this.event_calbacks[event]) this.event_calbacks[event] = [];

		let p = this.event_calbacks[event].indexOf(cb);
		if (p > -1) return;

		this.event_calbacks[event].push(cb);
		console.warn(
			"Handler added for " +
				event +
				"; " +
				this.event_calbacks[event].length +
				" total",
		);

		if (event.indexOf("/") === 0) {
			// topic or camera id
			this.createSubscriber(event);

			if (this.latest[event]) {
				setTimeout(() => {
					cb(this.latest[event].msg, this.latest[event].ev);
				}, 0);
			}
		}
	}

	once(event, cb) {
		if (!this.event_calbacks[event]) this.event_calbacks[event] = [];

		let wrapper_cb = (...args) => {
			this.off(event, wrapper_cb);
			setTimeout(() => {
				cb(...args);
			}, 0);
		};
		this.on(event, wrapper_cb);
	}

	off(event, cb) {
		// console.warn('unsubscribe', cb)
		if (!this.event_calbacks[event]) {
			console.warn("Event not registered", event, this.event_calbacks);
			return;
		}

		let p = this.event_calbacks[event].indexOf(cb);
		if (p !== -1) {
			this.event_calbacks[event].splice(p, 1);
			console.log(
				"Handler removed for " +
					event +
					"; " +
					this.event_calbacks[event].length +
					" remaing",
			);
			if (this.event_calbacks[event].length == 0) {
				delete this.event_calbacks[event];
				if (event.indexOf("/") === 0) {
					// topic or camera id
					console.log("Unsubscribing from " + event);
					this.removeSubscriber(event);
				}
			}
		} else {
			console.error("cb not found in unsubscribe for " + event, cb);
		}
	}

	emit(event, ...args) {
		if (!this.event_calbacks[event]) {
			// console.log('no callbacks for '+event);
			return;
		}

		// console.log('calling callbacks for '+event, this.event_calbacks[event]);
		this.event_calbacks[event].forEach((cb) => {
			setTimeout(() => {
				cb(...args);
			}, 0);
		});
	}

	onTopicConfig(topic, cb) {
		if (!this.topic_config_calbacks[topic]) this.topic_config_calbacks[topic] = [];

		this.topic_config_calbacks[topic].push(cb);

		if (this.topic_configs[topic]) {
			//if we have cofig, fire right away
			setTimeout(() => {
				cb(this.topic_configs[topic]);
			}, 0);
		}
	}

	removeTopicConfigHandler(topic, cb) {
		if (!this.topic_config_calbacks[topic]) return;

		let p = this.topic_config_calbacks[topic].indexOf(cb);
		if (p !== -1) {
			this.topic_config_calbacks[topic].splice(p, 1);
			console.log(
				"Handler removed for topic config " +
					topic +
					"; " +
					this.topic_config_calbacks[topic].length +
					" remaining",
			);
			if (this.topic_config_calbacks[topic].length == 0) {
				delete this.topic_config_calbacks[topic];
			}
		}
	}

	emitTopicConfig(topic, config) {
		if (!this.topic_config_calbacks[topic]) {
			return;
		}

		// console.log('calling callbacks for '+event, this.event_calbacks[event]);
		this.topic_config_calbacks[topic].forEach((cb) => {
			setTimeout(() => {
				cb(config);
			}, 0);
		});
	}

	createSubscriber(id_source) {
		if (this.subscribers[id_source]) {
			console.log(
				"Reusing subscriber for " +
					id_source +
					"; init_complete=" +
					this.init_complete,
			);
			return this.subscribers[id_source];
		}

		console.log(
			"Creating subscriber for " +
				id_source +
				"; init_complete=" +
				this.init_complete,
		);

		this.subscribers[id_source] = new Subscriber(this, id_source);

		if (this.init_complete) {
			//not waiting for initial bulk subs collection
			console.log("emitting subscribe for " + id_source);

			if (this.add_subscribers_timeout) {
				window.clearTimeout(this.add_subscribers_timeout);
				this.add_subscribers_timeout = null;
			}
			if (!this.queued_subs) this.queued_subs = [];
			this.queued_subs.push(id_source);
			this.add_subscribers_timeout = window.setTimeout(
				() => this.delayedBulkCreateSubscribers(),
				5,
			);
		}

		//init dc async
		return this.subscribers[id_source];
	}

	removeSubscriber(id_source) {
		if (!this.subscribers[id_source]) {
			console.log("Subscriber not found for " + id_source + "; not removing");
			return;
		}

		console.log("Removing subscriber for " + id_source);
		// this.topic_dcs

		delete this.subscribers[id_source];

		if (this.topic_streams[id_source]) {
			console.log("Clearing media stream for " + id_source);

			// not deleting media streams! same on the bridge node
			// if (this.media_streams[this.topic_streams[id_source]])
			//     delete this.media_streams[this.topic_streams[id_source]];

			// deleting data topic channel
			delete this.topic_streams[id_source];
		}

		// if (topic_dcs) {

		// }

		if (this.init_complete) {
			//not waiting for initial subs
			if (this.remove_subscribers_timeout) {
				window.clearTimeout(this.remove_subscribers_timeout);
				this.remove_subscribers_timeout = null;
			}
			if (!this.queued_unsubs) this.queued_unsubs = [];
			this.queued_unsubs.push(id_source);
			this.remove_subscribers_timeout = window.setTimeout(
				() => this.delayedBulkRemoveSubscribers(),
				5,
			);
		}
	}

	delayedBulkCreateSubscribers() {
		if (!this.can_change_subscriptions || this.requested_subscription_change) {
			// if (!this.can_change_subscriptions)
			//     console.warn('can_change_subscriptions=false');
			// if (this.requested_subscription_change)
			//     console.warn('requested_subscription_change=true');
			this.add_subscribers_timeout = window.setTimeout(
				() => this.delayedBulkCreateSubscribers(),
				10, // wait
			);
			return;
		}

		console.log("requesting subscribe to ", this.queued_subs);
		this.requested_subscription_change = true; //lock
		this.socket.emit(
			"subscribe",
			{
				id_robot: this.id_robot,
				sources: this.queued_subs,
			},
			(res) => {
				console.warn("Res sub", res);
				this.requested_subscription_change = false; //unlock
			},
		);
		this.queued_subs = [];
		this.add_subscribers_timeout = null;
	}

	delayedBulkRemoveSubscribers() {
		if (!this.can_change_subscriptions || this.requested_subscription_change) {
			this.remove_subscribers_timeout = window.setTimeout(
				() => this.delayedBulkRemoveSubscribers(),
				100, // wait
			);
			return;
		}

		console.log("requesting unsubscribe from ", this.queued_unsubs);

		this.socket.emit(
			"unsubscribe",
			{
				id_robot: this.id_robot,
				sources: this.queued_unsubs,
			},
			(res) => {
				console.warn("Res unsub", res);
				// this.requested_subscription_change = false; //unlock
			},
		);
		this.queued_unsubs = [];
		this.remove_subscribers_timeout = null;
	}

	getWriter(topic, msg_type, err_out = null) {
		if (this.topic_writers[topic] && this.topic_writers[topic].msg_type == msg_type) {
			console.log(
				"Re-using writer for " +
					topic +
					" and " +
					msg_type +
					"; init_complete=" +
					this.init_complete,
			);
			return this.topic_writers[topic];
		}

		if (this.topic_writers[topic] && this.topic_writers[topic].msg_type != msg_type) {
			console.error(
				"Will not write " +
					msg_type +
					" into " +
					topic +
					" (type mixing: " +
					this.topic_writers[topic].msg_type +
					" x " +
					msg_type +
					")",
			);
			if (err_out) {
				err_out.error = true;
				err_out.message =
					"Not writing because mixing message types in one topic breaks ROS. " +
					"Either change the ouput topic, or save your changes & reload this page to override.";
			}
			return false;
		}

		console.warn(
			"Beginning writing " +
				msg_type +
				" to " +
				topic +
				"; init_complete=" +
				this.init_complete,
		);

		this.topic_writers[topic] = new TopicWriter(this, topic, msg_type);
		let that = this;
		if (this.init_complete) {
			// not waiting for initial subs
			this.socket.emit(
				"subscribe:write",
				{
					id_robot: this.id_robot,
					sources: [[topic, msg_type]],
				},
				(res_pub) => {
					if (res_pub && res_pub.err) {
						console.error(
							"Error creating a topic writer for " + topic + ":",
							res_pub.err,
						);
					} else {
						console.warn("Res pub: ", res_pub);
						if (res_pub["write_data_channels"]) {
							res_pub["write_data_channels"].forEach((one_res) => {
								let res_topic = one_res[0];
								let dc_id = one_res[1];
								let res_msg_type = one_res[2];
								that._makeWriteDataChannel(
									res_topic,
									dc_id,
									res_msg_type,
								);
							});
						}
					}
				},
			);
		}
		return this.topic_writers[topic];
	}

	removeWriter(topic) {
		if (!this.writers[topic]) {
			console.log("Writer not found for " + topic + "; not removing");
			return;
		}

		console.log("Removing writer for " + topic);
		// this.topic_dcs

		if (this.writers[topic].dc) {
			this.writers[topic].dc.close();
		}
		delete this.writers[topic];

		if (this.init_complete) {
			//not waiting for initial subs
			this.socket.emit(
				"unsubscribe:write",
				{
					id_robot: this.id_robot,
					sources: [topic],
				},
				(res) => {
					console.warn("Res unpub", res);
				},
			);
		}
	}

	connect() {
		if (this.socket.connected) {
			console.log("Socket.io already connected");
			return;
		}

		console.log("Connecting Socket.io...");
		this.socket.connect();

		this.startHeartbeat(); // make webrtc data channel (at least one needed to open webrtc connection)
	}

	_initRobotConnection() {
		let that = this;

		let subscribe = Object.keys(this.subscribers);
		let writers = [];
		Object.keys(this.topic_writers).forEach((topic) => {
			writers.push([topic, that.topic_writers[topic].msg_type]);
		});
		this.init_complete = true;

		let req_data = {
			id_robot: this.id_robot,
			read: subscribe,
			write: writers,
		};

		console.log("Socket.io requesting robot connection with request:", req_data);

		this.socket.emit("robot", req_data, (robot_data) => {
			that._processRobotData(robot_data, (answer_data) => {
				that.socket.emit("sdp:answer", answer_data, (res_answer) => {
					if (!res_answer || !res_answer["success"]) {
						console.error(
							"Error answering topic read subscription offer: ",
							res_answer,
						);
						return;
					}
				});
			});
		});
	}

	disconnect() {
		if (!this.socket.connected) {
			console.log("Socket.io already disconnected");
			return;
		}

		console.log("Disconnecting Socket.io...");
		this.socket.disconnect();
	}

	startHeartbeat() {
		if (this.heartbeat_timer) return;

		this.heartbeat_logged = false;
		this.heartbeat_writer = this.getWriter("_heartbeat", "std_msgs/msg/Byte");

		if (!this.heartbeat_writer) console.error("Error setting up heartbeat writer");
		else {
			console.log("heartbeat writer ready");
			let that = this;
			that.heartbeat_timer = setInterval(() => {
				if (!that.pc || that.pc.connectionState != "connected") return;

				if (!that.heartbeat_writer.send({ data: 1 })) {
					// true when ready and written
					console.warn("Failed to send heartbeat");
					that.heartbeat_logged = false;
				} else if (!that.heartbeat_logged) {
					console.log("Heartbeat started");
					that.heartbeat_logged = true;
				}
			}, 5000);
		}
	}

	stopHeartbeat() {
		if (this.heartbeat_timer) {
			console.log("Heartbeat stopped");
			clearInterval(this.heartbeat_timer);
			this.heartbeat_timer = null;
		}
	}

	getBridgeFileUrl(url) {
		let res = this.bridge_files_url
			.replace("%ROBOT_ID%", this.id_robot)
			.replace("%SECRET%", this.bridge_files_secret)
			.replace("%URL%", encodeURIComponent(url));
		return res;
	}

	_loadExternalScript(url, class_to_load) {
		let that = this;
		return new Promise((resolve, reject) => {
			if (that.extrernal_scripts[url] !== undefined) {
				console.log("External script already included: " + url);
				return;
			}

			that.extrernal_scripts[url] = null; // now loading

			import(url)
				.then((imported_module) => {
					if (!imported_module || !imported_module[class_to_load]) {
						that.extrernal_scripts[url] = false;
						console.error("Class " + class_to_load + " not found in " + url);
						reject();
						return;
					}
					that.extrernal_scripts[url] = true;
					console.log(
						"Class " + class_to_load + " loaded from external " + url,
					);
					resolve(imported_module[class_to_load]);
				})
				.catch((error) => {
					that.extrernal_scripts[url] = false;
					reject(error);
				});
		});
	}

	_processRobotData(robot_data, answer_callback) {
		console.info("Recieved robot state data: ", this.id_robot, robot_data);
		let that = this;

		if (robot_data["session"]) {
			// no session means server pushed just brief info
			if (this.session != robot_data["session"]) {
				console.warn("NEW PC SESSION " + robot_data["session"]);
				if (this.session || this.pc) {
					this._clearConnection(); // closes the pc so that we can start a new one
				}
			}
			this.session = robot_data["session"];
		}

		let error = robot_data["err"] ? robot_data["err"] : null;
		if (error) {
			let msg = robot_data["msg"];
			console.error("Robot reply: " + msg);
			this.emit("error", error, msg);
		}

		if (robot_data["id_robot"] && this.id_robot != robot_data["id_robot"]) {
			console.error("Robot id missmatch: " + robot_data["id_robot"]);
			this.emit("error", 1, "Robot id mismatch: " + robot_data["id_robot"]);
			return;
		}

		this.name = robot_data["name"];
		let prev_online = this.robot_socket_online;
		if (robot_data["ip"]) {
			this.ip = robot_data["ip"];
			this.robot_socket_online = true;
		} else {
			this.robot_socket_online = false;
		}

		let prev_introspection = this.introspection;
		this.introspection = robot_data["introspection"];
		if (robot_data["files_fw_secret"]) {
			this.bridge_files_secret = robot_data["files_fw_secret"];
		}

		this.ros_distro = robot_data["ros_distro"];
		this.client_version = robot_data["git_tag"]
			? robot_data["git_tag"]
			: robot_data["git_sha"]
				? "#" + robot_data["git_sha"].slice(0, 7)
				: null;

		if (robot_data["ice_servers"]) {
			console.log("Got ice servers: ", robot_data["ice_servers"]);
			if (!robot_data["ice_servers"].length) {
				this.ui.showNotification("No ICE servers received", "error");
				console.error("No ICE servers received!");
			} else {
				let ice_servers_config = {
					urls: [],
				};
				if (robot_data["ice_username"])
					ice_servers_config["username"] = robot_data["ice_username"];
				if (robot_data["ice_secret"])
					ice_servers_config["credential"] = robot_data["ice_secret"];
				robot_data["ice_servers"].forEach((one) => {
					ice_servers_config["urls"].push(one);
				});
				// "turn:turn.phntm.io:3478",
				// "turn:turn.phntm.io:3479",
				this.ice_servers_config = [ice_servers_config];
			}
		}

		if (
			this.robot_socket_online &&
			!this.pc /*|| this.pc.signalingState == 'closed' */
		) {
			console.warn(
				`Creating new webrtc peer w id_instance=${this.socket_auth.id_instance}`,
			);
			this.pc = this._initPeerConnection(this.id_robot);
		} else if (this.robot_socket_online) {
			console.info(
				`Valid webrtc peer, robot_socket_online=${this.robot_socket_online} pc=${this.pc} id_instance=${this.socket_auth.id_instance}`,
			);
			// should autoconnect ?!?!
		}

		if (prev_online != this.robot_socket_online)
			this.emit("robot-socket-connected", this.robot_socket_online);

		this.emit("update"); //updates the ui

		if (prev_introspection != this.introspection)
			this.emit("introspection", this.introspection);

		if (!this.robot_socket_online) {
			// in case of socket connection loss this webrtc stays up transmitting p2p
			console.warn("Server reports robot disconnected from socket.io");
			this._clearConnection();
			return;
		}

		// load custom input drivers
		let loading_num_custom_drivers = 0;
		let input_drivers = null;
		let input_profile_defaults = null;
		let input_service_defaults = null;
		if (
			robot_data["custom_input_drivers"] &&
			robot_data["custom_input_drivers"].length
		) {
			robot_data["custom_input_drivers"].forEach((custom_driver) => {
				if (!custom_driver.class || !custom_driver.url) {
					console.error(
						"Invalid custom_input_driver definition received:",
						custom_driver,
					);
					return;
				}
				loading_num_custom_drivers++;
				this._loadExternalScript(custom_driver.url, custom_driver.class)
					.then((loaded_class) => {
						that.input_manager.registerDriver(
							custom_driver.class,
							loaded_class,
						);
						loading_num_custom_drivers--;
						if (loading_num_custom_drivers == 0)
							// all done?
							that.emit(
								"input_config",
								input_drivers,
								input_profile_defaults,
								input_service_defaults,
							);
					})
					.catch((error) => {
						console.error(
							"External input driver loading error for " +
								custom_driver.url,
							error,
						);
						that.ui.showNotification(
							"Error loading external input driver " + custom_driver.class,
							"error",
							'<pre>Source: <a href="' +
								custom_driver.url +
								'">' +
								custom_driver.url +
								"</a>\n" +
								error +
								"</pre>",
						);
						loading_num_custom_drivers--;
						if (loading_num_custom_drivers == 0)
							// all done?
							that.emit(
								"input_config",
								input_drivers,
								input_profile_defaults,
								input_service_defaults,
							);
					});
			});
		}

		if (
			robot_data["input_drivers"] ||
			robot_data["input_defaults"] ||
			robot_data["service_defaults"]
		) {
			input_drivers = robot_data["input_drivers"];
			input_profile_defaults = robot_data["input_defaults"]
				? robot_data["input_defaults"]
				: {};
			input_service_defaults = robot_data["service_defaults"]
				? robot_data["service_defaults"]
				: {};
			if (loading_num_custom_drivers == 0)
				// wait for all custom input drivers to load
				this.emit(
					"input_config",
					input_drivers,
					input_profile_defaults,
					input_service_defaults,
				); // service_buttons must trigger before ui
		}

		// load custom service widgets
		if (
			robot_data["custom_service_widgets"] &&
			robot_data["custom_service_widgets"].length
		) {
			robot_data["custom_service_widgets"].forEach((custom_widget) => {
				if (!custom_widget.class || !custom_widget.url) {
					console.error(
						"Invalid custom_service_widgets definition received:",
						custom_widget,
					);
					return;
				}
				// loading_num_custom_drivers++;
				this._loadExternalScript(custom_widget.url, custom_widget.class)
					.then((loaded_class) => {
						that.ui.addCustomServiceWidget(custom_widget.class, loaded_class);
					})
					.catch((error) => {
						console.error(
							"External service widget loading error for " +
								custom_widget.url,
							error,
						);
						that.ui.showNotification(
							"Error loading external service widget " +
								custom_widget.class,
							"error",
							'<pre>Source: <a href="' +
								custom_widget.url +
								'">' +
								custom_widget.url +
								"</a>\n" +
								error +
								"</pre>",
						);
					});
			});
		}

		if (robot_data["service_widgets"] && robot_data["service_widgets"].length) {
			robot_data["service_widgets"].forEach((srv_mapping) => {
				if (!srv_mapping.srv || !srv_mapping.class) {
					console.error(
						"Invalid service_widgets mapping received:",
						srv_mapping,
					);
					return;
				}
				that.ui.addServiceWidgetMapping(
					srv_mapping.srv,
					srv_mapping.class,
					srv_mapping.data,
				);
			});
		}

		if (robot_data["read_data_channels"]) {
			console.log("Got read channels", robot_data["read_data_channels"]);
			robot_data["read_data_channels"].forEach((topic_data) => {
				let topic = topic_data[0];
				let dc_id = topic_data[1];
				let msg_type = topic_data[2];
				let reliable = topic_data[3];
				let topic_config = topic_data[4]; //extra topic config

				if (
					dc_id &&
					that.topic_readers[topic] &&
					that.topic_readers[topic].msg_type == msg_type
				) {
					that.topic_readers[topic].dc_id = dc_id;
				}
				if (dc_id && msg_type) {
					// dc_ids starts with 1
					that._makeReadDataChannel(topic, dc_id, msg_type, reliable);
				} else if (
					topic &&
					that.topic_readers[topic] &&
					that.topic_readers[topic].dc
				) {
					console.log("Topic " + topic + " closed by the robot");
					that.topic_readers[topic].dc.close();
					that.topic_readers[topic].dc = null;
					delete that.topic_readers[topic];
				}

				if (topic_config && Object.keys(topic_config).length) {
					console.log("Got " + topic + " extra config:", topic_config);
					that.topic_configs[topic] = topic_config;
					that.emitTopicConfig(topic, that.topic_configs[topic]);
				} else if (that.topic_configs[topic]) {
					console.log("Deleted " + topic + " extra config");
					delete that.topic_configs[topic];
					that.emitTopicConfig(topic, null);
				}
			});
		}

		if (robot_data["write_data_channels"]) {
			console.log("Got write channels", robot_data["write_data_channels"]);
			robot_data["write_data_channels"].forEach((topic_data) => {
				let topic = topic_data[0];
				let dc_id = topic_data[1];
				let msg_type = topic_data[2];

				if (
					dc_id &&
					that.topic_writers[topic] &&
					that.topic_writers[topic].msg_type == msg_type
				) {
					that.topic_writers[topic].dc_id = dc_id;
				}
				if (dc_id && msg_type) {
					// dc_ids starts with 1
					that._makeWriteDataChannel(topic, dc_id, msg_type);
				} else if (
					topic &&
					that.topic_writers[topic] &&
					that.topic_writers[topic].dc
				) {
					console.log("Topic " + topic + " closed by the robot");
					that.topic_writers[topic].dc.close();
					that.topic_writers[topic].dc = null;
				}
			});
		}

		if (robot_data["read_video_streams"]) {
			robot_data["read_video_streams"].forEach((stream_data) => {
				let id_src = stream_data[0];
				let id_stream = stream_data[1];

				if (Array.isArray(id_stream) && id_stream.length > 1) {
					id_stream = id_stream[0];
					let src_type = id_stream[1]; // sensor_msgs/msg/Image
				}

				if (id_stream) {
					// stream open
					if (
						!that.topic_streams[id_src] ||
						that.topic_streams[id_src] != id_stream
					) {
						console.log("Setting stream to " + id_stream + " for " + id_src);
						that.topic_streams[id_src] = id_stream;
					} else {
						console.log(
							"Stream already in use for " +
								id_src +
								"; old=" +
								that.topic_streams[id_src] +
								" new=" +
								id_stream +
								"",
						);
					}

					if (that.media_streams[id_stream]) {
						console.log(
							"Re-using stream for " +
								id_src +
								"; " +
								that.topic_streams[id_src],
						);
						that.emit("media_stream", id_src, that.media_streams[id_stream]);
					}
				} else if (that.topic_streams[id_src]) {
					//stream closed
					console.log(
						"Stream closed for " + id_src + "; " + that.topic_streams[id_src],
					);

					// not deleting media stream!
					// if (this.media_streams[this.topic_streams[id_source]])
					//     delete this.media_streams[this.topic_streams[id_source]];

					// deleting data topic channel
					delete that.topic_streams[id_src];
				}
			});
		}

		if (robot_data["ui"]) {
			console.log("UI got config: ", robot_data["ui"]);
			this.emit("ui_config", robot_data["ui"]); // must trigger after service_buttons
		}

		if (robot_data["offer"] && answer_callback) {
			console.log("Got sdp offer", robot_data["offer"]);
			let robot_offer = new RTCSessionDescription({
				sdp: robot_data["offer"],
				type: "offer",
			});

			that.pc.setRemoteDescription(robot_offer).then(() => {
				that.pc.createAnswer().then((answer) => {
					that.pc.setLocalDescription(answer).then(() => {
						that._waitForIceGathering()
							.then(() => {
								console.log(
									"Ice cool, state=",
									that.pc.iceGatheringState,
								);
								console.log("Ice servers:", that.ice_servers_config);
								// console.log('First local description:', that.pc.localDescription.sdp);

								let answer_data = {
									id_robot: that.id_robot,
									sdp: that.pc.localDescription.sdp,
								};
								console.log("Sending sdp asnwer", answer_data.sdp);
								answer_callback(answer_data);
							})
							.catch(() => {
								console.error("Error handling robot's offer");
							});
					});
				});
			});
		} else {
			console.log("Processed robot data without a SDP Offer, unlocking subs..");
			this.can_change_subscriptions = true;
		}
	}

	_iceChecker(resolve, reject, start_time) {
		if (this.pc && this.pc.iceGatheringState == "complete") return resolve();

		if (start_time === undefined) start_time = Date.now();
		else if (Date.now() - start_time > 30000) {
			if (this.pc)
				console.error(
					"Timed out while waiting for ICE gathering, state=" +
						this.pc.iceGatheringState,
				);
			else
				console.error("Timed out while waiting for ICE gathering, pc=" + this.pc);
			if (reject) return reject();
			return;
		}

		if (this.pc)
			console.log("Waiting for ICE gathering, state=" + this.pc.iceGatheringState);
		else console.log("Waiting for ICE gathering, pc=" + this.pc);

		let that = this;
		setTimeout(() => {
			that._iceChecker(resolve, reject, start_time);
		}, 100);
	}

	_waitForIceGathering() {
		let that = this;
		return new Promise((resolve, reject) => {
			return that._iceChecker(resolve, reject);
		});
	}

	_makeReadDataChannel(topic, dc_id, msg_type, reliable) {
		if (this.topic_readers[topic] && dc_id == this.topic_readers[topic].dc_id) {
			console.log("Reader already exists for " + topic + "; dc_id=" + dc_id);
			return;
		} else if (this.topic_readers[topic]) {
			//non-matching dc id
			delete this.topic_readers[topic];
		}

		if (this.pc.signalingState == "closed") {
			console.err(
				"Cannot create read DC for " + topic + "; pc.signalingState=closed",
			);
			return;
		}

		let dc = null;
		try {
			console.log(
				"Creating read DC#" +
					dc_id +
					" for " +
					topic +
					"; reliable=" +
					reliable +
					"; pc=",
				this.pc,
			);
			dc = this.pc.createDataChannel(topic, {
				negotiated: true, // negotiated by the app, not webrtc layer
				ordered: reliable ? true : false,
				maxRetransmits: reliable ? null : 0,
				id: dc_id,
			});
		} catch (e) {
			console.error("Creating read DC for " + topic + " failed", e);
			return;
		}

		let reader = new TopicReader({
			topic: topic,
			msg_type: msg_type,
			dc_id: dc_id,
			dc: dc,
			msg_reader: null,
			msg_queue: [], //stores early messages until we have msg_type definition
			logged: false,
		});
		this.topic_readers[topic] = reader;

		let that = this;
		dc.addEventListener("open", (open_evt) => {
			console.log(
				"Read channel " + topic + " open " + open_evt.target.label,
				open_evt,
			);
		});
		dc.addEventListener("error", (err_evt) => {
			if (that.socket.connected)
				console.error(
					"Read channel " + topic + " error " + err_evt.target.label,
					err_evt,
				);
		});
		dc.addEventListener("bufferedamountlow", (event) => {
			console.warn(
				"Read channel " + topic + " bufferedamountlow " + event.target.label,
				event,
			);
		});
		dc.addEventListener("close", (close_evt) => {
			console.log("Read channel " + topic + " close", close_evt);
			delete that.topic_readers[topic];
		});
		dc.addEventListener("message", (msg_evt) => {
			if (!reader.logged) {
				console.info("Read channel " + topic + " receiving messages");
				reader.logged = true;
			}

			if (!reader.tryGetMessageReader(that)) {
				reader.msg_queue.push(msg_evt);
				return;
			}
			that._onDCMessage(reader, msg_evt);
		});
	}

	_makeWriteDataChannel(topic, dc_id, msg_type) {
		if (!this.topic_writers[topic]) {
			console.log("Writer not found for " + topic);
			return;
		}

		if (this.topic_writers[topic].dc && this.topic_writers[topic].dc.id == dc_id) {
			console.log("Writer DC already exists for " + topic + "; dc_id=" + dc_id);
			return; //we cool here
		}

		let dc = null;
		try {
			console.log("Creating write DC for " + topic + "; pc=", this.pc);
			dc = this.pc.createDataChannel(topic, {
				negotiated: true, // negotiated by the app, not webrtc layer
				ordered: false,
				maxRetransmits: 0,
				id: dc_id,
			});
			this.topic_writers[topic].dc = dc;
		} catch (e) {
			console.error("Creating write DC for " + topic + " failed", e);
			return;
		}

		let that = this;
		let is_heartbeat = topic == "_heartbeat";
		dc.addEventListener("open", (ev) => {
			console.warn("Write DC " + topic + " open", dc.id);
		});
		dc.addEventListener("close", (ev) => {
			console.warn("Write DC " + topic + " closed", dc.id);
			if (that.topic_writers[topic].dc == dc) {
				delete that.topic_writers[topic].dc;
				that.topic_writers[topic].dc_id = -1;
			}
		});
		dc.addEventListener("error", (ev) => {
			if (that.socket.connected) console.error("write DC " + topic + " error", ev);
			if (that.topic_writers[topic].dc == dc) {
				delete that.topic_writers[topic].dc;
				that.topic_writers[topic].dc_id = -1;
			}
		});
		dc.addEventListener("message", (msg_evt) => {
			if (is_heartbeat) {
				//console.info("Heartbeat got PONG");
				return;
			}
			console.warn("Write DC " + topic + " got message");
		});
	}

	_clearConnection() {
		console.warn("Clearing session");
		this.session = null; // pc session
		//this.socket_auth.id_instance = null; // Bridge Server will generate new instance id on connection
		// this.init_complete = false;

		let that = this;
		Object.keys(this.topic_writers).forEach((topic) => {
			if (that.topic_writers[topic].dc) {
				that.topic_writers[topic].dc.close(); // deleted in dc close handler
			}
		});
		Object.keys(this.topic_readers).forEach((topic) => {
			if (that.topic_readers[topic].dc) {
				that.topic_readers[topic].dc.close(); // deleted in dc close handler
			}
		});
		this.media_streams = {};
		this.topic_streams = {};
		if (this.pc != null) {
			this.pc.close(); // make new pc
			this.pc = null;
		}

		this.emit("peer_disconnected"); // updates the ui
	}

	_onDCMessage(dc, msg_evt) {
		//arraybuffer

		if (!dc.msg_reader) return;

		let rawData = msg_evt.data;

		let decoded = null;
		let raw_len = 0;
		let raw_type = "";
		let that = this;

		if (rawData instanceof ArrayBuffer) {
			raw_len = rawData.byteLength;
			raw_type = "ArrayBuffer";
			let v = new DataView(rawData);
			decoded = dc.msg_reader.readMessage(v);
		} else if (rawData instanceof Blob) {
			// firefox uses Blobs
			raw_len = rawData.size;
			raw_type = "Blob";

			new Response(rawData).arrayBuffer().then((buff) => {
				let v = new DataView(buff);
				decoded = dc.msg_reader.readMessage(v);
				that.emit(dc.topic, decoded, msg_evt);
				that.latest[dc.topic] = {
					msg: decoded,
					ev: msg_evt,
				};
			});
			return; //async
		} else {
			// consider string
			decoded = rawData;
		}

		if (!dc.logged) {
			dc.logged = true;
			console.log(
				"Incoming data for " + dc.topic + " (" + dc.msg_type + ")",
				decoded,
			);
		}

		this.emit(dc.topic, decoded, msg_evt);
		that.latest[dc.topic] = {
			msg: decoded,
			ev: msg_evt,
		};
	}

	_initPeerConnection(id_robot) {
		let config = {
			sdpSemantics: "unified-plan",
			iceServers: this.ice_servers_config,
			bundlePolicy: "max-compat",
		};

		if (this.force_turn) {
			console.warn("Forcing TURN connection...");
			config["iceTransportPolicy"] = "relay"; //force TURN
		}

		let pc = new RTCPeerConnection(config);
		// pc.addTransceiver('video', { direction: 'recvonly' });
		let that = this;

		pc.addEventListener("icegatheringstatechange", (evt) => {
			console.warn("Ice gathering state changed: ", pc.iceGatheringState);
		});

		pc.addEventListener("iceconnectionstatechange", (evt) => {
			console.warn("Ice connection state changed: ", pc.iceConnectionState);
		});

		pc.addEventListener("negotiationneeded", (evt) => {
			console.warn("Negotiation needed! ");
		});

		// connect audio / video
		pc.addEventListener("track", (evt) => {
			console.warn("New track added: ", evt);

			for (let i = 0; i < evt.streams.length; i++) {
				let stream = evt.streams[i];

				that.media_streams[stream.id] = stream;

				Object.keys(that.topic_streams).forEach((id_src) => {
					if (that.topic_streams[id_src] == stream.id) {
						that.emit("media_stream", id_src, stream);
					}
				});

				stream.addEventListener("addtrack", (evt) => {
					console.info("Stream added track " + stream.id, evt);
				});
				stream.addEventListener("removetrack", (evt) => {
					console.error("Stream removed track " + stream.id, evt);
				});
				stream.addEventListener("onactive", (evt) => {
					console.warn("Stream active " + stream.id, evt);
				});
				stream.addEventListener("oninactive", (evt) => {
					console.error("Stream inactive " + stream.id, evt);
				});
			}

			evt.track.addEventListener("ended", (evt) => {
				console.warn("Track ended!", evt, evt.target.id);
			});
		});

		// data channels opened by the app, not webrtc
		pc.addEventListener("datachannel", (evt) => {
			console.log("New read data channel added " + ch.label);
		});

		pc.addEventListener("negotiationneeded", (evt) => {
			console.warn("negotiationneeded!", evt);
		});

		pc.addEventListener("signalingstatechange", (evt) => {
			console.warn("signalingstatechange", pc.signalingState);

			that.can_change_subscriptions = pc.signalingState == "stable";
		});

		pc.addEventListener("connectionstatechange", (evt) => {
			let newState = evt.currentTarget.connectionState;
			console.log("Peer connection state: ", newState);

			if (newState == "connected") {
				if (!pc.connected) {
					//just connected
					pc.connected = true;
					that.emit("peer_connected");
					that.runPeerStatsLoop();
				}
			} else if (newState == "disconnected" || newState == "failed") {
				//just disconnected

				console.warn(
					`Peer disconnected, robot_socket_online=${that.robot_socket_online}`,
				);

				// let was_connected = pc.connected;
				that.peer_stats_loop_running = false;
				pc.connected = false;
				that.emit("peer_disconnected"); // updates the ui & sends webrtc state to robot
			}

			if (newState == "failed") {
				console.log("Connection failed, clearing...", evt);
				that._clearConnection();
			}

			that.emit("peer_conection_changed");
		});

		this.emit("peer_conection_changed"); // updates the ui & sends webrtc state to robot

		return pc;
	}

	getTURNConnectionInfo() {
		if (
			this.pc &&
			this.pc.connectionState == "connected" &&
			this.pc.sctp &&
			this.pc.sctp.transport &&
			this.pc.sctp.transport.iceTransport
		) {
			let selectedPair =
				this.pc.sctp.transport.iceTransport.getSelectedCandidatePair();
			if (selectedPair && selectedPair.remote) {
				console.log("Remote peer: ", selectedPair.remote);
				return [selectedPair.remote.type, selectedPair.remote.address];
			} else {
				return [null, null];
			}
		} else {
			return [null, null];
		}
	}

	// post connection update to Bridge Server
	// this is only used in for system diagnostics
	reportConectionState() {
		let con_data = {
			id_robot: this.id_robot,
			state: this.pc ? this.pc.connectionState : "n/a",
		};
		if (this.pc && this.pc.connectionState == "connected") {
			const [remote_type, remote_ip] = this.getTURNConnectionInfo();
			con_data["method"] = remote_type;
			con_data["ip"] = remote_ip;
		}
		this.socket.emit("wrtc-info", con_data);
	}

	async getPeerStats() {
		let that = this;
		return new Promise((resolve) => {
			if (!that.pc || !that.pc.connected) {
				setTimeout(() => {
					resolve();
				}, 100);
				return;
			}

			that.pc
				.getStats(null)
				.then((results) => {
					that.emit("peer_stats", results);
				})
				.catch((err) => {
					console.error(err);
				})
				.finally((info) => {
					setTimeout(() => {
						resolve();
					}, 1000);
				});
		});
	}

	async runPeerStatsLoop() {
		this.peer_stats_loop_running = true;

		while (this.peer_stats_loop_running) {
			await this.getPeerStats();
		}
	}

	registerServiceRequestCallback(id_service, async_cb) {
		if (!this.service_request_callbacks[id_service])
			this.service_request_callbacks[id_service] = [];
		if (this.service_request_callbacks[id_service].indexOf(async_cb) < 0)
			this.service_request_callbacks[id_service].push(async_cb);
	}

	registerServiceReplyCallback(id_service, cb) {
		if (!this.service_reply_callbacks[id_service])
			this.service_reply_callbacks[id_service] = [];
		if (this.service_reply_callbacks[id_service].indexOf(cb) < 0)
			this.service_reply_callbacks[id_service].push(cb);
	}

	serviceCall(service, data, silent_req, cb) {
		let that = this;
		if (
			this.service_request_callbacks[service] &&
			this.service_request_callbacks[service].length
		) {
			let num_completed = 0;
			let abort = false;
			for (let i = 0; i < this.service_request_callbacks[service].length; i++) {
				this.service_request_callbacks[service][i](data, (cb_res) => {
					if (!cb_res) {
						abort = true;
						console.log("Service call " + service + " aborted by cb");
					}

					num_completed++;
					if (num_completed == this.service_request_callbacks[service].length) {
						if (!abort) {
							that._doServiceCall(service, data, silent_req, cb);
						} else {
							if (cb) cb();
						}
					}
				});
			}
		} else {
			this._doServiceCall(service, data, silent_req, cb);
		}
	}

	_doServiceCall(service, data, silent_req, cb) {
		let req = {
			id_robot: this.id_robot,
			service: service,
			msg: data, // data undefined => no msg
		};
		if (this.ui && !silent_req) {
			//let data_hr = (data !== null && data !== undefined);
			this.ui.showNotification(
				"Calling " + service,
				null,
				"Request data:<br><pre>" + JSON.stringify(data, null, 2) + "</pre>",
			);
		}
		let that = this;
		console.log("Service call request", req);
		this.socket.emit("service", req, (reply) => {
			console.log("Service call reply", reply);

			if (that.service_reply_callbacks[service]) {
				for (let i = 0; i < that.service_reply_callbacks[service].length; i++) {
					that.service_reply_callbacks[service][i](data, reply);
				}
			}

			if (cb) cb(reply);
		});
	}

	runIntrospection(state = true) {
		if (this.introspection == state) return;
		this.socket.emit(
			"introspection",
			{ id_robot: this.id_robot, state: state },
			(res) => {
				if (!res || !res["success"]) {
					console.error("Introspection start err: ", res);
					return;
				}
			},
		);
	}

	findMessageType(search, msg_types) {
		if (msg_types === undefined) msg_types = this.supported_msg_types;
		if (!msg_types) return null;

		for (let i = 0; i < msg_types.length; i++) {
			if (msg_types[i].name == search) {
				return msg_types[i];
			}
		}
		return null;
	}
}
