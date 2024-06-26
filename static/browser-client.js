class TopicWriter {
    constructor(client, topic, msg_type, on_ready_cb) {
        this.client = client;
        this.topic = topic;
        this.msg_type = msg_type;
        this.ui = null;
        
        if (!client.msg_writers[msg_type]) {
            let Writer = window.Serialization.MessageWriter;
            let msg_class = client.find_message_type(msg_type);
            if (!msg_class) {
                console.error('Failed creating writer for '+msg_type+'; message class not found');
                return null;
            }
            client.msg_writers[msg_type] = new Writer( [ msg_class ].concat(client.supported_msg_types) );
        }

        this.msg_writer = client.msg_writers[msg_type];
        this.dc = null;
        this.on_ready_cb = on_ready_cb;
    }

    send(msg) {
        if (!this.client.pc || this.client.pc.connectionState != 'connected')
            return false; //not ready

        if (!this.dc || this.dc.readyState != 'open') {
            // if (this.dc)
            //     console.warn('Writer dc not ready for '+topic+': '+(this.dcs[topic] ? 'state='+this.dcs[topic].readyState : 'Not initiated'))
            return false; //not yer ready
        }

        if (!this.msg_writer) {
            return false; //err
        }

        //console.log('Writing '+msg_type+' into '+topic, this.dcs[topic])
        setTimeout(()=>{
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

export function IsImageTopic(t) {
    return t == 'sensor_msgs/msg/Image' ||
           t == 'sensor_msgs/msg/CompressedImage' ||
           t == 'ffmpeg_image_transport_msgs/msg/FFMPEGPacket';
}

export function IsFastVideoTopic(t) {
    return t == 'ffmpeg_image_transport_msgs/msg/FFMPEGPacket';
}

export class PhntmBridgeClient extends EventTarget {

    id_robot = null;
    session = null;
    supported_msg_types = null; //fetched static

    pc = null;
    pc_connected = false;

    discovered_topics = {} // str topic => { msg_types: str[], subscribed: bool }
    topic_dcs = {}; //str topic => RTCDataChannel
    // let topic_video_tracks = {}; //str topic => MediaStreamTrack
    // let topic_transceivers = {}; //str topic => RTCRtpTransceiver
    discovered_nodes = {}; // str service => { msg_type: str}
    discovered_services = {}; // str service => { msg_type: str}
    discovered_cameras = {}; // str id => { info: {}}
    discovered_docker_containers = {}; // str id => { info: {}}

    topic_configs = {};
    topic_streams = {}; // topic/cam => id_stream
    media_streams = {}; // id_stream => MediaStream

    transievers = []; // RTCRtpTransceiver[]
    // let topic_media_streams = {}; // str topic => MediaStream

    preferedVideoCodecs = [];

    socket = null;
    event_calbacks = {}

    constructor(opts) {
        super();

        this.id_robot = opts.id_robot;
        if (!this.id_robot) {
            console.error("Missing opts.id_robot")
            return false;
        }
        this.app_id = opts.app_id;
        if (!this.app_id) {
            console.error("Missing opts.app_id")
            return false;
        }
        this.app_key = opts.app_key;
        if (!this.app_key) {
            console.error("Missing opts.app_key")
            return false;
        }
        this.session = null;

        //defaults to phntm bridge service
        this.socket_url = opts.socket_url !== undefined ? opts.socket_url : 'https://bridge.phntm.io:1337';
        this.socket_path = opts.socket_path !== undefined ? opts.socket_path : '/app/socket.io/';
        this.socket_auto_connect = opts.socket_auto_connect !== undefined ? opts.socket_auto_connect : false;

        this.bridge_files_url = opts.bridge_files_url;
        this.bridge_files_secret = null;

        this.ice_servers_config = opts.ice_servers ? opts.ice_servers : [{urls:[
            "stun:stun.l.google.com:19302",
        ]}];
        this.force_turn = opts.force_turn; 

        this.init_complete = false;
        this.msg_writers = {}; //msg_type => writer
        this.msg_readers = {}; //msg_type => reader
        this.topic_writers = {}; //id => writer
        this.subscribers = {}; //id => topic ot cam reader
        this.can_change_subscriptions = false;
        this.requested_subscription_change = false;
        this.topic_streams = {};
        this.topic_configs = {}; //id => extra topic config
        this.media_streams = {}; //id_stream => stream
        this.latest = {};

        this.supported_msg_types = null;
        this.msg_types_src = opts.msg_types_src !== undefined ? opts.msg_types_src : '/static/msg_types.json' // json defs generated from .idl files
        this.load_message_types(); //async

        let that = this;

        this.socket_auth = {
            id_app: this.app_id,
            key: this.app_key,
            id_instance: null, // stored for reconnects when known
        }

        this.socket = io(this.socket_url, {
            path: this.socket_path,
            auth: this.socket_auth,
            autoConnect: this.socket_auto_connect,
            forceNew: true,
            transport: 'websocket'
        });

        this.socket.on("connect", () => {

            let subscribe = Object.keys(that.subscribers);
            let writers = [];
            Object.keys(that.topic_writers).forEach((topic)=>{
                writers.push([ topic, that.topic_writers[topic].msg_type ]);
            })
            that.init_complete = true;

            let req_data = {
                id_robot: that.id_robot,
                read: subscribe,
                write: writers,
            }

            console.log('Socket.io connected with id '+that.socket.id+', requesting:', req_data);

            setTimeout(()=>{
                that.socket.emit('robot', req_data,
                    (robot_data) => {
                        that._process_robot_data(robot_data, (answer_data) => {
                            that.socket.emit('sdp:answer', answer_data, (res_answer) => {
                                if (!res_answer || !res_answer['success']) {
                                    console.error('Error answering topic read subscription offer: ', res_answer);
                                    return;
                                }
                            });
                        });
                    }
                );
            }, 0);
        });

        this.socket.on("disconnect", () => {
            console.log('Socket.io disconnected'); // undefined
            that.robot_online = false;
            setTimeout(()=>{
                that.emit('socket_disconnect');
            }, 0);
        });

        this.socket.on("error", (err) => {
            console.error('Socket.io error', err); // undefined
        });

        this.socket.on("connect_error", (err) => {
            console.error('Socket.io connect error:', err); // undefined
        });

        this.socket.on('instance', (id_instance) => {
            setTimeout(()=>{
                if (that.socket_auth.id_instance != id_instance) {
                    console.warn('Got new id instance: '+id_instance);
                    if (that.pc) {
                        console.warn('Removing pc for old instance: '+that.socket_auth.id_instance);
                        that.pc.close();
                        that.pc = null;
                    }
                    that.socket_auth.id_instance = id_instance;
                }
            }, 0);
        });

        this.socket.on('robot', (robot_data, return_callback) => {
            setTimeout(()=>{
                that._process_robot_data(robot_data, return_callback);
            }, 0)
        });

        this.socket.on("robot:update", (update_data, return_callback) => {
            setTimeout(()=>{
                that._process_robot_data(update_data, return_callback);
            }, 0);
        });

        this.socket.on("introspection", (state) => {
            console.log('Got introspetion state '+state); // undefined
            that.introspection = state;
            setTimeout(()=>{
                that.emit('introspection', that.introspection);
            }, 0);
        });

        this.socket.on('nodes', (nodes_data) => {

            if (!nodes_data[this.id_robot])
                return;

            console.warn('Raw nodes: ', nodes_data);

            setTimeout(()=>{
                this.discovered_nodes = {};
                Object.keys(nodes_data[this.id_robot]).forEach((node)=>{
                    this.discovered_nodes[node] = {
                        node: node,
                        namespace: nodes_data[this.id_robot][node]['namespace'],
                        publishers: {},
                        subscribers: {},
                        services: {},
                    }
                    if (nodes_data[this.id_robot][node]['publishers']) {
                        let topics = Object.keys(nodes_data[this.id_robot][node]['publishers']);
                        topics.forEach((topic) => {
                            let msg_types = nodes_data[this.id_robot][node]['publishers'][topic];
                            this.discovered_nodes[node].publishers[topic] = {
                                msg_types: msg_types,
                                is_video: IsImageTopic(msg_types[0]),
                                msg_type_supported: this.find_message_type(msg_types[0]) != null,
                            }
                        })
                    }
                    if (nodes_data[this.id_robot][node]['subscribers']) {
                        let topics = Object.keys(nodes_data[this.id_robot][node]['subscribers']);
                        topics.forEach((topic) => {
                            let msg_types = nodes_data[this.id_robot][node]['subscribers'][topic];
                            this.discovered_nodes[node].subscribers[topic] = {
                                msg_types: msg_types,
                                is_video: IsImageTopic(msg_types[0]),
                                msg_type_supported: this.find_message_type(msg_types[0]) != null,
                            }
                        })
                    }
                    if (nodes_data[this.id_robot][node]['services']) {
                        let services = Object.keys(nodes_data[this.id_robot][node]['services']);
                        services.forEach((service) => {
                            let msg_types = nodes_data[this.id_robot][node]['services'][service];
                            this.discovered_nodes[node].services[service] = {
                                service: service,
                                msg_types: msg_types,
                            }
                        })
                    }
                });

                console.log('Got nodes ', this.discovered_nodes);
                this.emit('nodes', this.discovered_nodes);

            }, 0);

            // let i = 0;
            // let subscribe_topics = [];
            // Object.keys(topics_data).forEach((id_robot) => {

            //     if (!topics_data[id_robot])
            //         return;

            //     //sort by topic
            //     topics_data[id_robot].sort((a, b) => {
            //         if (a.topic < b.topic) {
            //             return -1;
            //         }
            //         if (a.topic > b.topic) {
            //             return 1;
            //         }
            //         // a must be equal to b
            //         return 0;
            //     });

            //     $('#topics_heading').html(topics_data[id_robot].length+' Topics');


            // });


            // if (subscribe_topics.length)
            //     SetTopicsReadSubscription(id_robot, subscribe_topics, true);


        });


        this.socket.on('topics', (topics_data) => {

            if (!topics_data[this.id_robot])
                return;

            setTimeout(()=>{
                this.discovered_topics = {};
                topics_data[this.id_robot].forEach((topic_data)=>{
                    let topic = topic_data.shift();
                    let msg_types = topic_data
                    this.discovered_topics[topic] = {
                        msg_types: msg_types,
                        id: topic,
                        is_video: IsImageTopic(msg_types[0]),
                        msg_type_supported: this.find_message_type(msg_types[0]) != null,
                    }
                });

                console.log('Got topics ', this.discovered_topics);
                this.emit('topics', this.discovered_topics);
            }, 0);

            // let i = 0;
            // let subscribe_topics = [];
            // Object.keys(topics_data).forEach((id_robot) => {

            //     if (!topics_data[id_robot])
            //         return;

            //     //sort by topic
            //     topics_data[id_robot].sort((a, b) => {
            //         if (a.topic < b.topic) {
            //             return -1;
            //         }
            //         if (a.topic > b.topic) {
            //             return 1;
            //         }
            //         // a must be equal to b
            //         return 0;
            //     });

            //     $('#topics_heading').html(topics_data[id_robot].length+' Topics');


            // });


            // if (subscribe_topics.length)
            //     SetTopicsReadSubscription(id_robot, subscribe_topics, true);


        });

        this.socket.on('services', (services_data) => {

            if (!services_data[this.id_robot])
                return;

            setTimeout(()=>{
                this.discovered_services = {};

                // let i = 0;
                services_data[this.id_robot].forEach((service_data) => {
                    let service = service_data[0];
                    let msg_type = service_data[1];
                    this.discovered_services[service] = {
                        service: service,
                        msg_type: msg_type
                    };
                });

                console.log('Got services:', this.discovered_services);
                this.emit('services', this.discovered_services);
            }, 0);
        });

        this.socket.on('cameras', (cameras_data) => {

            if (!cameras_data[this.id_robot])
                return;

            setTimeout(()=>{
                this.discovered_cameras = {};

                Object.keys(cameras_data[this.id_robot]).forEach((id_camera) => {

                    this.discovered_cameras[id_camera] = {
                        id: id_camera,
                        info: cameras_data[this.id_robot][id_camera],
                    };
                });

                console.log('Got Cameras:', this.discovered_cameras);
                this.emit('cameras', this.discovered_cameras);
            }, 0);
        });

        this.socket.on('docker', (docker_containers_data) => {

            if (!docker_containers_data[this.id_robot])
                return;

            setTimeout(()=>{
                this.discovered_docker_containers = {};

                docker_containers_data[this.id_robot].forEach((cont_data) => {
                    this.discovered_docker_containers[cont_data.id] = cont_data
                });

                console.log('Got Docker containers:', this.discovered_docker_containers);
                this.emit('docker', this.discovered_docker_containers);
            }, 0);
        });

        window.addEventListener("beforeunload", function(e){
            if (that.pc) {
                console.warn('Unloading window, disconnecting pc...');
                that.pc.close();
                that.pc = null;
            }
        });

        this.on('peer_connected', () => this.start_heartbeat());
        this.on('peer_disconnected', () => this.stop_heartbeat());
        // pc = InitPeerConnection(id_robot);
    }

    on(event, cb) {

        if (!this.event_calbacks[event])
            this.event_calbacks[event] = [];

        let p = this.event_calbacks[event].indexOf(cb);
        if (p > -1)
            return;

        this.event_calbacks[event].push(cb);

        if (event.indexOf('/') === 0) {  // topic or camera id
            this.create_subscriber(event);

            if (this.latest[event]) {
                setTimeout(()=>{
                    cb(this.latest[event].msg, this.latest[event].ev);
                }, 0);
            }
        }
    }

    once(event, cb) {
        if (!this.event_calbacks[event])
            this.event_calbacks[event] = [];

        let wrapper_cb = (...args) => {
            this.off(event, wrapper_cb);
            setTimeout(()=>{
                cb(...args);
            }, 0);
        }
        this.on(event, wrapper_cb)
    }

    off(event, cb) {
        // console.warn('unsubscribe', cb)
        if (!this.event_calbacks[event]) {
            console.warn('Event not registered', event, this.event_calbacks)
            return;
        }

        let p = this.event_calbacks[event].indexOf(cb)
        if (p !== -1) {
            this.event_calbacks[event].splice(p, 1);
            console.log('Handler removed for '+event+"; "+Object.keys(this.event_calbacks[event]).length+" remaing");
            if (Object.keys(this.event_calbacks[event]).length == 0) {
                delete this.event_calbacks[event];
                if (event.indexOf('/') === 0) { // topic or camera id
                    console.log('Unsubscribing from '+event);
                    this.remove_subscriber(event);
                }
            }
        }
        else {
            console.error('cb not found in unsubscribe for '+event, cb)
        }
    }

    emit(event, ...args) {
        if (!this.event_calbacks[event]) {
            // console.log('no callbacks for '+event);
            return;
        }

        // console.log('calling callbacks for '+event, this.event_calbacks[event]);
        let callbacks = Object.values(this.event_calbacks[event]);
        callbacks.forEach((cb) => {
            setTimeout(()=>{
                cb(...args)
            }, 0);
        });
    }

    create_subscriber(id_source) {

        if (this.subscribers[id_source]) {
            console.log('Reusing subscriber for '+id_source+'; init_complete='+this.init_complete);
            return this.subscribers[id_source];
        }

        console.log('Creating subscriber for '+id_source+'; init_complete='+this.init_complete)

        this.subscribers[id_source] = new Subscriber(this, id_source);

        if (this.init_complete) { //not waiting for initial subs
            console.log('emitting subscribe for '+id_source)

            if (this.add_subscribers_timeout) {
                window.clearTimeout(this.add_subscribers_timeout);
                this.add_subscribers_timeout = null;
            }
            if (!this.queued_subs)
                this.queued_subs = [];
            this.queued_subs.push(id_source);
            this.add_subscribers_timeout = window.setTimeout(
                () => this.delayed_bulk_create_subscribers(),
                5
            );
        }

        //init dc async
        return this.subscribers[id_source];
    }

    remove_subscriber(id_source) {

        if (!this.subscribers[id_source]) {
            console.log('Subscriber not found for '+id_source+'; not removing')
            return;
        }

        console.log('Removing subscriber for '+id_source);
        // this.topic_dcs

        delete this.subscribers[id_source];

        if (this.topic_streams[id_source]) {
            console.log('Clearing media stream for '+id_source);
            // if (this.media_streams[this.topic_streams[id_source]])
            //     delete this.media_streams[this.topic_streams[id_source]];
            delete this.topic_streams[id_source];
        }

        // if (topic_dcs) {
            
        // }

        if (this.init_complete) { //not waiting for initial subs
            if (this.remove_subscribers_timeout) {
                window.clearTimeout(this.remove_subscribers_timeout);
                this.remove_subscribers_timeout = null;
            }
            if (!this.queued_unsubs)
                this.queued_unsubs = [];
            this.queued_unsubs.push(id_source);
            this.remove_subscribers_timeout = window.setTimeout(
                () => this.delayed_bulk_remove_subscribers(),
                5
            );
        }
    }

    delayed_bulk_create_subscribers () {

        if (!this.can_change_subscriptions || this.requested_subscription_change) {
            this.add_subscribers_timeout = window.setTimeout(
                () => this.delayed_bulk_create_subscribers(),
                100 // wait
            );
            return;
        }

        console.log('requesting subscribe to ', this.queued_subs);
        this.requested_subscription_change = true; //lock
        this.socket.emit('subscribe', {
            id_robot: this.id_robot,
            sources: this.queued_subs
        }, (res) => {
            console.warn('Res sub', res);
            this.requested_subscription_change = false; //unlock
        });
        this.queued_subs = [];
        this.add_subscribers_timeout = null;
    }

    delayed_bulk_remove_subscribers () {

        if (!this.can_change_subscriptions || this.requested_subscription_change) {
            this.remove_subscribers_timeout = window.setTimeout(
                () => this.delayed_bulk_remove_subscribers(),
                100 // wait
            );
            return;
        }

        console.log('requesting unsubscribe from ', this.queued_unsubs);
        
        this.socket.emit('unsubscribe', {
            id_robot: this.id_robot,
            sources: this.queued_unsubs
        }, (res) => {
            console.warn('Res unsub', res);
            this.requested_subscription_change = false; //unlock
        });
        this.queued_unsubs = [];
        this.remove_subscribers_timeout = null;
    }

    get_writer(topic, msg_type, on_ready_cb, err_out = null) {

        if (this.topic_writers[topic] && this.topic_writers[topic].msg_type == msg_type) {
            console.log('Re-using writer for '+topic+' and '+msg_type+'; init_complete='+this.init_complete);
            return this.topic_writers[topic];
        }

        if (this.topic_writers[topic] && this.topic_writers[topic].msg_type != msg_type) {
            console.error('Will not write '+msg_type+' into '+topic+' (type mixing)');
            if (err_out) {
                err_out.error = true;
                err_out.message = 'Not writing because mixing message types in one topic breaks ROS.' +
                                  'Either change the ouput topic, or save your changes & reload this page to override.';
            }
            return false;
        }

        console.warn('Beginning writing '+msg_type+' to '+topic+'; init_complete='+this.init_complete)

        this.topic_writers[topic] = new TopicWriter(this, topic, msg_type, on_ready_cb);

        if (this.init_complete) { //not waiting for initial subs
            this.socket.emit('subscribe:write', {
                id_robot: this.id_robot,
                sources: [[ topic, msg_type ]]
            },  (res_sub) => {
                console.warn('Res pub: ', res_sub);
                if (res_sub && res_sub.err) {
                    console.error('Res pub err: ', res_sub.err);
                }
            });
        }
        return this.topic_writers[topic];
    }

    remove_writer(topic) {
        if (!this.writers[topic]) {
            console.log('Writer not found for '+topic+'; not removing')
            return;
        }

        console.log('Removing writer for '+topic);
        // this.topic_dcs

        delete this.writers[topic];

        if (this.init_complete) { //not waiting for initial subs
            this.socket.emit('unsubscribe:write', {
                id_robot: this.id_robot,
                sources: [ topic ]
            }, (res) => {
                console.warn('Res unpub', res);
            });
        }
    }

    load_message_types() {

        fetch(this.msg_types_src)
        .then((response) => response.json())
        .then((json) => {
            this.supported_msg_types = json;
            console.log('Fetched '+json.length+' msg types from '+this.msg_types_src)
            this.emit('message_types_loaded');
        });
    }

    // when_message_types_loaded(cb) {
    //     if (this.supported_msg_types === null) {
    //         this.once('message_types_loaded', () => {
    //             console.log('message_types_loaded');
    //             cb();
    //         });
    //     } else {
    //         cb();
    //     }
    // }

    connect() {
        if (this.supported_msg_types === null) {
            //wait for messages defs to load
            this.once('message_types_loaded', () => { this.connect(); });
            return;
        }

        this.start_heartbeat(); // make webrtc data channel (at least one needed to open webrtc connection)

        console.log('Connecting Socket.io...')
        this.socket.connect();
    }

    start_heartbeat() {

        if (this.heartbeat_timer)
            return;

        this.heartbeat_logged = false;
        this.heartbeat_writer = this.get_writer('_heartbeat', 'std_msgs/msg/Byte');

        if (!this.heartbeat_writer)
            console.error('Error setting up heartbeat writer');
        else {
            console.log('heartbeat writer ready');
            let that = this;
            that.heartbeat_timer = setInterval(()=>{

                if (!that.pc || that.pc.connectionState != 'connected')
                    return;

                if (!that.heartbeat_writer.send({data: 1})) { // true when ready and written
                    console.warn('Failed to send heartbeat');
                    that.heartbeat_logged = false;
                } else if (!that.heartbeat_logged) {
                    console.log('Heartbeat started');
                    that.heartbeat_logged = true;
                }

            }, 3000);
        }
    }

    stop_heartbeat() {
        if (this.heartbeat_timer) {
            console.log('Heartbeat stopped');
            clearInterval(this.heartbeat_timer);
            this.heartbeat_timer = null;
        }
    }

    get_bridge_file_url(url) {
        let res = this.bridge_files_url
                    .replace('%ROBOT_ID%', this.id_robot)
                    .replace('%SECRET%', this.bridge_files_secret)
                    .replace('%URL%', encodeURIComponent(url));
        return res;
    }

    _process_robot_data(robot_data, answer_callback) {

        console.warn('Recieved robot state data: ', this.id_robot, robot_data);

        if (robot_data['session']) { // no session means server pushed just brief info
            if (this.session != robot_data['session']) {
                console.warn('NEW PC SESSION '+robot_data['session'])
                if (this.pc != null) {
                    this.pc.close(); // make new pc
                    this.pc = null;
                }
            }
            this.session = robot_data['session'];
        }
    

        let error = robot_data['err'] ? robot_data['err'] : null;
        if (error) {
            let msg = robot_data['msg']
            console.error('Robot reply: '+msg);
            this.emit('error', error, msg);
        }

        if (robot_data['id_robot'] && this.id_robot != robot_data['id_robot']) {
            console.error('Robot id missmatch: '+robot_data['id_robot']);
            this.emit('error', 1, 'Robot id mismatch: '+robot_data['id_robot']);
            return;
        }

        this.name = robot_data['name'];
        let prev_online = this.robot_online;
        this.robot_online = robot_data['ip'] ? true : false;
        this.ip = robot_data['ip'];
        let prev_introspection = this.introspection;
        this.introspection = robot_data['introspection'];
        if (robot_data['files_fw_secret']) {
            this.bridge_files_secret = robot_data['files_fw_secret'];
        }
            

        if (this.robot_online && !this.pc /*|| this.pc.signalingState == 'closed' */) {
            console.warn(`Creating new webrtc peer w id_instance=${this.socket_auth.id_instance}`);
            this.pc = this._init_peer_connection(this.id_robot);
        } else {
            console.warn(`Re-using old webrtc peer, robot_online=${this.robot_online} pc=${this.pc} id_instance=${this.socket_auth.id_instance}; this should autoconnect...`);
            // should autoconnect ?!?!
        }

        if (prev_online != this.robot_online)
            this.emit('online', this.robot_online);
        this.emit('update');
        if (prev_introspection != this.introspection)
            this.emit('introspection', this.introspection);

        // if (robot_online && (!pc || pc.connectionState != 'connected')) {
        //     WebRTC_Negotiate(robot_data['id_robot']);
        // }

        if (robot_data['input_drivers'] || robot_data['input_defaults']) {
            let drivers = robot_data['input_drivers'];
            let defaults = robot_data['input_defaults'];
            this.emit('input_config', drivers, defaults);
        }

        // if (robot_data['gp_drivers'] || robot_data['gp_defaults']) {
        //     let drivers =  robot_data['gp_drivers'];
        //     let defaults = robot_data['gp_defaults'];
        //     this.emit('gp_config', drivers, defaults);
        // }

        // if (robot_data['touch_drivers'] || robot_data['touch_defaults']) {
        //     let drivers =  robot_data['touch_drivers'];
        //     let defaults = robot_data['touch_defaults'];
        //     this.emit('touch_config', drivers, defaults);
        // }

        if (robot_data['read_data_channels']) {
            robot_data['read_data_channels'].forEach((topic_data)=>{
                let topic = topic_data[0];
                let dc_id = topic_data[1];
                let msg_type = topic_data[2];
                let reliable = topic_data[3];
                let topic_config = topic_data[4]; //extra topic config

                if (topic_config && Object.keys(topic_config).length) {
                    console.log('Got '+topic+' extra config:', topic_config);
                    this.topic_configs[topic] = topic_config;
                    this.emit('topic_config', topic, this.topic_configs[topic]);
                } else if (this.topic_configs[topic]) {
                    console.log('Deleted '+topic+' extra config');
                    delete this.topic_configs[topic];
                    this.emit('topic_config', topic, null);
                }
                
                // if (dc_id && msg_type) {
                //     this._make_read_data_channel(topic, dc_id, msg_type, reliable)
                // } else if (topic && this.topic_dcs[topic]) {
                //     console.log('Topic '+topic+' closed by the client')
                //     this.topic_dcs[topic].close();
                //     delete this.topic_dcs[topic];
                // }
            });
        }

        if (robot_data['ui']) {
            console.log('ui got config: ', robot_data['ui']);
            this.emit('ui_config', robot_data['ui']);
        }

        if (robot_data['read_video_streams']) {
            robot_data['read_video_streams'].forEach((stream_data)=>{
                let id_src = stream_data[0];
                let id_stream = stream_data[1];

                if (Array.isArray(id_stream) && id_stream.length > 1) {
                    id_stream = id_stream[0];
                    let src_type = id_stream[1] // sensor_msgs/msg/Image
                }

                if (id_stream) {
                    if (!this.topic_streams[id_src] || this.topic_streams[id_src] != id_stream) {
                        console.log('Setting stream to '+id_stream+' for '+id_src);
                        this.topic_streams[id_src] = id_stream;
                        if (this.media_streams[id_stream]) {
                            this.emit('media_stream', id_src, this.media_streams[id_stream]);
                        }
                    } else {
                        console.log('Stream already exists for '+id_src +'; old='+this.topic_streams[id_src]+' new='+id_stream+'');
                    }

                } else if (this.topic_streams[id_src]) {
                    //stream closed
                    console.log('Stream closed for '+id_src +'; '+this.topic_streams[id_src]);
                    // if (this.media_streams[this.topic_streams[id_src]])
                    //     delete this.media_streams[this.topic_streams[id_src]];
                    delete this.topic_streams[id_src];
                }
            });
        }

        if (robot_data['write_data_channels']) {
            robot_data['write_data_channels'].forEach((topic_data)=>{
                let topic = topic_data[0];
                let dc_id = topic_data[1];
                let msg_type = topic_data[2];
                if (dc_id && msg_type) {
                    this._make_write_data_channel(topic, dc_id, msg_type)
                } else if (topic && this.topic_writers[topic] && this.topic_writers[topic].dc) {
                    console.log('Topic '+topic+' closed by the client')
                    this.topic_writers[topic].dc.close();
                    this.topic_writers[topic].dc = null;
                }
            });
        }

        if (robot_data['offer'] && answer_callback) {
            console.log('Got sdp offer', robot_data['offer'])
            let robot_offer = new RTCSessionDescription({ sdp: robot_data['offer'], type: 'offer' });
            let that = this;

            that.pc.setRemoteDescription(robot_offer).then(() => {
                that.pc.createAnswer().then((answer) => {
                    that.pc.setLocalDescription(answer)
                    .then(()=>{

                        that._wait_for_ice_gathering().then(()=>{
                            console.log('Ice cool, state=', that.pc.iceGatheringState);
                            console.log('Ice servers:', that.ice_servers_config);
                            // console.log('First local description:', that.pc.localDescription.sdp);

                            let answer_data = {
                                id_robot: that.id_robot,
                                sdp: that.pc.localDescription.sdp,
                            };
                            console.log('Sending sdp asnwer', answer_data.sdp)
                            answer_callback(answer_data);

                        }).catch(()=>{
                            console.error('Error handling robot\'s offer');
                        });
                       
                    });
                });
            });
        } else {
            console.log('Initiated without subs, unlocking...');
            this.can_change_subscriptions = true;
        }


        // server reports robot disconnect
        // in case of socket connection loss this webrtc stays up transmitting p2p
        if (!this.robot_online && this.pc && this.pc_connected) {
            // console.warn('Robot offline, restarting pc...');
            // this.pc.close();
            // const ev = new Event("connectionstatechange");

            // for (const topic of Object.values(topics)) {

            //     topic.subscribed = false;

            //     if (topic.id_stream && media_streams[topic.id_stream]) {
            //         media_streams[topic.id_stream].getTracks().forEach(track => track.stop());
            //         delete media_streams[topic.id_stream];

            //         if (panels[topic.id]) {
            //             console.log('Closing video panel for '+topic.id, document.getElementById('panel_video_'+panels[topic.id].n));
            //             document.getElementById('panel_video_'+panels[topic.id].n).srcObject = undefined;
            //         }
            //     }
            // }

            // for (const cam of Object.values(cameras)) {
            //     cam.subscribed = false;

            //     if (cam.id_stream && media_streams[cam.id_stream]) {
            //         media_streams[cam.id_stream].getTracks().forEach(track => track.stop());
            //         delete media_streams[cam.id_stream];

            //         if (panels[cam.id]) {
            //             console.log('Closing video panel for '+cam.id, document.getElementById('panel_video_'+panels[cam.id].n));
            //             document.getElementById('panel_video_'+panels[cam.id].n).srcObject = undefined;
            //         }
            //     }
            // }

            // pc.dispatchEvent(ev);
        }




    }

    _ice_checker(resolve, reject, start_time) {
        if (this.pc && this.pc.iceGatheringState == 'complete')
            return resolve();

        if (start_time === undefined)
            start_time = Date.now();
        else if (Date.now() - start_time > 30000) {
            console.error('Timed out while waiting for ICE gathering, state='+this.pc.iceGatheringState);
            if (reject)
                return reject();
            return;
        }

        console.log('Waiting for ICE gathering, state='+this.pc.iceGatheringState);

        // Simulating a delayed network call to the server
        let that = this;
        setTimeout(() => {
            that._ice_checker(resolve, reject, start_time);
        }, 100);
    }

    _wait_for_ice_gathering() {
        let that = this;
        return new Promise((resolve, reject) => {
            return that._ice_checker(resolve, reject);
        });
    }

    _make_read_data_channel(topic, dc_id, msg_type, reliable) {

        if (this.topic_dcs[topic]) {
            console.log('DC already exists for '+topic);
            // if (dc_id != this.topic_dcs[topic].id) {
            //     console.warn('DC for '+topic+' has new id: '+dc_id+', old='+this.topic_dcs[topic].id);
            //     this.topic_dcs[topic].close();
            //     delete this.topic_dcs[topic];
            // } else {
            return; //we cool here
            // }
        }

        console.log('Creating DC for '+topic+'; reliable='+reliable);

        if (this.pc.signalingState == 'closed') {
            return console.err('Cannot create read DC for '+topic+'; pc.signalingState=closed');
        }

        let dc = this.pc.createDataChannel(topic, {
            negotiated: true,
            ordered: reliable ? true : false,
            maxRetransmits: reliable ? null : 0,
            id: dc_id
        });

        let Reader = window.Serialization.MessageReader;
        // console.warn('reader=', Reader);
        let msg_type_class = this.find_message_type(msg_type, this.supported_msg_types)
        let msg_reader = new Reader( [ msg_type_class ].concat(this.supported_msg_types) );
        // console.warn('reader inst=', msg_reader);
        this.topic_dcs[topic] = dc;

        let that = this;

        dc.addEventListener('open', (ev)=> {
            console.warn('DC '+topic+' open', dc)
        });
        dc.addEventListener('close', (ev)=> {
            console.warn('DC '+topic+' close')
            delete that.topic_dcs[topic];
        });
        dc.addEventListener('error', (ev)=> {
            console.error('DC '+topic+' error', ev)
            delete that.topic_dcs[topic]
        });
        let dc_incomming_logged = false;
        dc.addEventListener('message', (ev)=> {

            // if (topic == '/tf_static' || topic == '/robot_description') {
            //     console.error(topic+': ', ev.data)
            // }

            let rawData = ev.data; //arraybuffer
            let decoded = null;
            let raw_len = 0;
            let raw_type = ""

            if (!dc_incomming_logged) {
                console.info('Incoming data for '+topic);
                dc_incomming_logged = true;
            }

            if (rawData instanceof ArrayBuffer ) {
                raw_len = rawData.byteLength;
                raw_type = 'ArrayBuffer';
                if (msg_reader != null) {                    
                    let v = new DataView(rawData)
                    decoded = msg_reader.readMessage(v);
                } else {
                    decoded = buf2hex(rawData)
                }
            } else if (rawData instanceof Blob) { //firefox
                raw_len = rawData.size;
                raw_type = 'Blob';
                if (msg_reader != null) {                    
                
                    new Response(rawData).arrayBuffer()
                    .then((buff)=>{
                        let v = new DataView(buff)
                        decoded = msg_reader.readMessage(v);
                        that.emit(topic, decoded, ev)
                        that.latest[topic] = {
                            msg: decoded,
                            ev: ev
                        };
                    });
                    return; //async

                } else {
                    decoded = buf2hex(rawData)
                }
                
            } else { // fail => string
                decoded = rawData; 
            }

            that.emit(topic, decoded, ev)
            that.latest[topic] = {
                msg: decoded,
                ev: ev
            };
        });
    }

    _make_write_data_channel(topic, dc_id, msg_type) {

        if (!this.topic_writers[topic]) {
            console.log('Writer not found for '+topic);
            return;
        }

        if (this.topic_writers[topic].dc) {
            console.log('Write DC already exists for '+topic);
            // if (dc_id != this.topic_writers[topic].dc.id) {
            //     console.warn('Write DC for '+topic+' has new id: '+dc_id+', old='+this.topic_writers[topic].dc.id);
            //     this.topic_writers[topic].dc.close();
            //     delete this.topic_writers[topic].dc
            // } else {
             return; //we cool here
            // }
        }

        console.log('Creating write DC for '+topic);

        let dc = this.pc.createDataChannel(
            topic,
            {
                negotiated: true,
                ordered: false,
                maxRetransmits: 0,
                id:dc_id
            }
        );

        this.topic_writers[topic].dc = dc;

        let that = this;
        dc.addEventListener('open', (ev)=> {
            console.warn('Write DC '+topic+' open', dc)
        });
        dc.addEventListener('close', (ev)=> {
            console.warn('Write DC '+topic+' closed')
            delete that.topic_writers[topic].dc;
        });
        dc.addEventListener('error', (ev)=> {
            console.error('write DC '+topic+' error', ev)
            delete that.topic_writers[topic].dc;
        });
    }


    _init_peer_connection(id_robot) {

        let config = {
            sdpSemantics: 'unified-plan',
            iceServers: this.ice_servers_config,
            // bundlePolicy: 'max-compat'
        };

        if (this.force_turn) {
            console.warn("Forcing TURN connection...")
            config['iceTransportPolicy'] = 'relay' //force TURN
        }

        let pc = new RTCPeerConnection(config);
        let that = this;

        // pc_.createDataChannel('_ignore_'); //wouldn't otherwise connect when initiated from the client

        // const capabilities = RTCRtpReceiver.getCapabilities('video');

        // capabilities.codecs.forEach(codec => {
        //      if (codec.mimeType == 'video/H264') {
        //          preferedVideoCodecs.push(codec);
        //      }
        // });
        // console.info('Video codecs: ', capabilities);
        // console.warn('Preferred video codecs: ', preferedVideoCodecs);
        //transceiver.setCodecPreferences(capabilities.codecs);

        // for (let i = 0; i < MAX_OPEN_VIDEO_STREAMS; i++) { //we need to prepare transcievers in advance before creating offer
        //     transievers.push(pc_.addTransceiver('video', {direction: 'recvonly'}).setCodecPreferences(preferedVideoCodecs));
        // }

        // let t = pc_.addTransceiver('video', {direction: 'recvonly'});
        // t.setCodecPreferences(preferedVideoCodecs);
        // transievers.push(t);

        //transievers.push(pc_.addTransceiver('video', {direction: 'recvonly'}));

        //pc_.addTransceiver('video', {direction: 'recvonly'}); //wouldn't otherwise open media streams (?)
        //pc_.addTransceiver('video', {direction: 'recvonly'}); //wouldn't otherwise open media streams (?)


        // pc.addTransceiver('audio', {direction: 'recvonly'});
        // data_receiver = pc.createDataChannel('test')
        // data_receiver.addEventListener("open", (evt) => {
        //     console.log('data_receiver.open', evt)
        // });
        // data_receiver.addEventListener("error", (evt) => {
        //     console.log('data_receiver.error', evt)
        // });
        // data_receiver.addEventListener("message", (evt) => {
        //     console.log('data_receiver.MSG:', evt)
        // });
        //ordered=true, protocol='test.protocol/lala.hm'

        pc.addEventListener('icegatheringstatechange', (evt) => {
            console.warn('Ice gathering state changed: ', pc.iceGatheringState);
        });

        pc.addEventListener('iceconnectionstatechange', (evt) => {
            console.warn('Ice connection state changed: ', pc.iceConnectionState);
        });

        pc.addEventListener('negotiationneeded', (evt) => {
            console.warn('Negotiation needed! ');
        });

        // connect audio / video
        pc.addEventListener('track', (evt) => {

            console.log('New track added: ', evt);

            //document.getElementById('panel_video_1').srcObject = evt.streams[0];

            for (let i = 0; i < evt.streams.length; i++) {
                let stream = evt.streams[i];

                that.media_streams[stream.id] = stream;

                Object.keys(that.topic_streams).forEach((id_src)=>{
                    if (that.topic_streams[id_src] == stream.id) {
                        that.emit('media_stream', id_src, stream)
                    }
                })

                stream.addEventListener('addtrack', (evt) => {
                    console.warn('Stream added track '+stream.id, evt);
                });
                stream.addEventListener('removetrack', (evt) => {
                    console.info('Stream removed track '+stream.id, evt);
                });
                stream.addEventListener('onactive', (evt) => {
                    console.info('Stream active '+stream.id, evt);
                });
                stream.addEventListener('oninactive', (evt) => {
                    console.info('Stream inactive '+stream.id, evt);
                });
            }

            //document.getElementById('panel_video_'+track.id).srcObject = evt.streams[0];
            //$('video').attr('src', evt.streams[0]);

            evt.track.addEventListener('ended', (evt) => {
                console.warn('Track ended!', evt);
            })
        });

        //let receiveChannel =
        // on_ready_cb

        // connect data
        pc.addEventListener('datachannel', (evt) => {

            let receiveChannel = evt.channel;
            let topic = receiveChannel.label;
            let channelLogged = false;

            let Reader = window.Serialization.MessageReader;
            let msg_type = receiveChannel.protocol;
            let msg_type_class = that.find_message_type(msg_type, that.supported_msg_types)
            if (!msg_type_class) {
                console.error('Unsupported msg type: '+msg_type);
                return;
            }
                
            let msg_reader = new Reader([ msg_type_class ].concat(that.supported_msg_types));

            receiveChannel.addEventListener("open", (open_evt) => {
                console.log('receiveChannel.open '+open_evt.target.label, open_evt)
            });
            receiveChannel.addEventListener("error", (err_evt) => {
                console.error('receiveChannel.error '+err_evt.target.label, err_evt)
            });
            receiveChannel.addEventListener("bufferedamountlow", (event) => {
                console.warn('receiveChannel.bufferedamountlow '+event.target.label, event)
            });

            receiveChannel.addEventListener("close", (close_evt) => { console.log('receiveChannel.close', close_evt) });
            
            receiveChannel.addEventListener("message", (msg_evt) => {
                
                let rawData = msg_evt.data; //arraybuffer
                let decoded = null;
                let raw_len = 0;
                let raw_type = "";

                if (rawData instanceof ArrayBuffer ) {
                    raw_len = rawData.byteLength;
                    raw_type = 'ArrayBuffer';
                    if (msg_reader != null) {                    
                        let v = new DataView(rawData)
                        decoded = msg_reader.readMessage(v);
                    } else {
                        decoded = buf2hex(rawData)
                    }
                } else if (rawData instanceof Blob) { //firefox
                    raw_len = rawData.size;
                    raw_type = 'Blob';
                    if (msg_reader != null) {                    
                    
                        new Response(rawData).arrayBuffer()
                        .then((buff)=>{
                            let v = new DataView(buff)
                            decoded = msg_reader.readMessage(v);
                            that.emit(topic, decoded, ev)
                            that.latest[topic] = {
                                msg: decoded,
                                ev: ev
                            };
                        });
                        return; //async
    
                    } else {
                        decoded = buf2hex(rawData)
                    }
                    
                } else { // fail => string
                    decoded = rawData; 
                }

                if (!channelLogged) {
                    channelLogged = true;
                    console.log('Incoming data for '+topic+' ('+msg_type+')', decoded);
                }

                that.emit(topic, decoded, msg_evt)
                that.latest[topic] = {
                    msg: decoded,
                    ev: msg_evt
                };
                
            });

            console.log('New data channel added '+receiveChannel.label, receiveChannel);

            // if (evt.track.kind == 'video') {
            //     document.getElementById('video').srcObject = evt.streams[0];
            // } else {
            //     document.getElementById('audio').srcObject = evt.streams[0];
            // }

        });

        pc.addEventListener('negotiationneeded', (evt) => {
            console.log('negotiationneeded!', evt);
        });

        pc.addEventListener('signalingstatechange', (evt) => {
            console.warn('signalingstatechange', pc.signalingState);

            that.can_change_subscriptions = (pc.signalingState == 'stable');

            switch (pc.signalingState) {
                case "closed":
                  console.warn('Peer connection closed');
                  pc = null;
                  break;
              }
        });

        pc.addEventListener("connectionstatechange", (evt) => {

            let newState = evt.currentTarget.connectionState;
            if (newState == 'failed' || newState == 'disconnected') {
                console.error('Peer connection state: ', evt.currentTarget.connectionState);
            } else {
                console.warn('Peer connection state: ', evt.currentTarget.connectionState);
            }

            if (evt.currentTarget.connectionState == 'connected') {
                if (!pc.connected) { //just connected
                    pc.connected = true;
                    setTimeout(()=>{
                        that.emit('peer_connected')
                    }, 0);
                    // window.gamepadController.InitProducers()
                    // let subscribe_topics = []
                    // let panelTopics = Object.keys(panels);
                    // for (let i = 0; i < panelTopics.length; i++) {
                    //     let topic = panelTopics[i];
                    //     if (topics[topic] && !topic_dcs[topic]) { //if we don't have topics[topic], it'll get subscribed on 'topics' event
                    //         subscribe_topics.push(topic);
                    //     }
                    // }
                    // if (subscribe_topics.length)
                    //     SetTopicsReadSubscription(id_robot, subscribe_topics, true);
                    that.run_peer_stats_loop();
                }
            } else if (evt.currentTarget.connectionState != 'connecting' && pc.connected) { //just disconnected

                console.error(`Peer disconnected, robot_online=${that.robot_online }`);

                let was_connected = pc.connected;
                that.peer_stats_loop_running = false;
                pc.connected = false;

                if (was_connected)
                    that.emit('peer_disconnected');

                // Object.keys(that.subscribers).forEach((sub) => {

                // });

                if (!that.robot_online && (!that.socket || !that.socket.connected)) {
                    console.warn('Clearing peer connection & instance id');
                    that.pc.close();
                    that.pc = null;
                    that.socket_auth.id_instance = null; // cloud bridge will generate new instance id on connection
                }

                // return;

                // window.gamepadController.ClearProducers();

                // for (const topic of Object.values(topics)) {

                //     topic.subscribed = false;

                //     if (topic.id_stream && media_streams[topic.id_stream]) {
                //         media_streams[topic.id_stream].getTracks().forEach(track => track.stop());
                //         delete media_streams[topic.id_stream];

                //         if (panels[topic.id]) {
                //             console.log('Closing video panel for '+topic.id, document.getElementById('panel_video_'+panels[topic.id].n));
                //             document.getElementById('panel_video_'+panels[topic.id].n).srcObject = undefined;
                //         }
                //     }
                // }

                // for (const cam of Object.values(cameras)) {
                //     cam.subscribed = false;

                //     if (cam.id_stream && media_streams[cam.id_stream]) {
                //         media_streams[cam.id_stream].getTracks().forEach(track => track.stop());
                //         delete media_streams[cam.id_stream];

                //         if (panels[cam.id]) {
                //             console.log('Closing video panel for '+cam.id, document.getElementById('panel_video_'+panels[cam.id].n));
                //             document.getElementById('panel_video_'+panels[cam.id].n).srcObject = undefined;
                //         }
                //     }
                // }

                // if (pc) {
                //     pc.close();
                //     pc = null;
                //     // pc = InitPeerConnection(id_robot); //prepare for next connection
                // }

            }
        });

        return pc;
    }
    
    async get_peer_stats() {
        let that = this;
        return new Promise((resolve) => {

            if (!that.pc || !that.pc.connected) {
                setTimeout(() => {
                    resolve();
                }, 100);
            }
                
            that.pc.getStats(null)
                .then((results) => {
                    that.emit('peer_stats', results);
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

    async run_peer_stats_loop() {
        // return;

        this.peer_stats_loop_running = true;

        while (this.peer_stats_loop_running) {
            await this.get_peer_stats();
        }
    }

    service_call(service, data, cb) { 
        let req = {
            id_robot: this.id_robot,
            service: service,
            msg: data // data undefined => no msg
        }
        console.warn('Service call request', req);
        this.socket.emit('service', req, (reply)=> {
            console.log('Service call reply', reply);
            if (cb)
                cb(reply);
        });
    }

    docker_container_start(id_cont, cb) { //
       this._docker_call(id_cont, 'start', cb)
    }

    docker_container_stop(id_cont, cb) { //
        this._docker_call(id_cont, 'stop', cb)
    }

    docker_container_restart(id_cont, cb) { //
        this._docker_call(id_cont, 'restart', cb)
    }

    _docker_call(id_cont, msg, cb) { //
        let req = {
            id_robot: this.id_robot,
            container: id_cont,
            msg: msg
        }
        console.warn('Docker request', req);
        this.socket.emit('docker', req, (reply)=> {
            console.log('Docker reply', reply);
            if (cb)
                cb(reply);
        });
    }

    run_introspection(state=true) {
        if (this.introspection == state)
            return;
        this.socket.emit('introspection', { id_robot: this.id_robot, state: state }, (res) => {
            if (!res || !res['success']) {
                console.error('Introspection start err: ', res);
                return;
            }
        });
    }

    wifi_scan(roam=true, cb) {
        console.warn('Triggering wifi scan on robot '+this.id_robot+'; roam='+roam)
        this.socket.emit('iw:scan', { id_robot: this.id_robot, roam: roam }, (res) => {
            if (!res || !res['success']) {
                console.error('Wifi scan err: ', res);
                if (cb)
                    cb(res);
                return;
            }
            console.log('IW Scan results:', res.res);
            if (cb)
                cb(res.res);
        });
    }

    find_message_type(search, msg_types) {
        if (msg_types === undefined)
            msg_types = this.supported_msg_types;

        for (let i = 0; i < msg_types.length; i++) {
            if (msg_types[i].name == search) {
                return msg_types[i];
            }
        }
        return null;
    }
}

function GetFile(url) {
    alert('TODO:\n'+url+'');
}















// function WebRTC_Negotiate(id_robot)
// {
//     console.log('WebRTC negotiating... ');

//     if (!pc)
//         pc = InitPeerConnection(id_robot);

//     return pc.createOffer().then(function(offer) {
//         return pc.setLocalDescription(offer);
//     }).then(function() {
//         // wait for ICE gathering to complete
//         return new Promise(function(resolve) {
//             if (pc.iceGatheringState === 'complete') {
//                 resolve();
//             } else {
//                 function checkState() {
//                     if (pc.iceGatheringState === 'complete') {
//                         pc.removeEventListener('icegatheringstatechange', checkState);
//                         resolve();
//                     }
//                 }
//                 pc.addEventListener('icegatheringstatechange', checkState);
//             }
//         });
//     }).then(function() {
//         let offer = pc.localDescription;
//         console.log('ICE gathering done, sending local offer: ', offer)
//         socket.emit('offer', { 'id_robot': id_robot, 'sdp': offer.sdp}, (answer) => {
//             if (answer.err) {
//                 console.error('Offer returned error', answer);
//                 return;
//             }
//             console.log('Setting remote answer:', answer.sdp);
//             return pc.setRemoteDescription({ sdp:answer.sdp, type:'answer'});
//         });
//     });

// }

// let topics_to_subscribe = []; // str topic
// let topics_to_unsubscribe = []; // str topic
// function SetTopicsReadSubscription(id_robot, topics_list, subscribe) {

//     for (let i = 0; i < topics_list.length; i++) {
//         let topic = topics_list[i];
//         if (subscribe) {
//             let pSubscribe = topics_to_subscribe.indexOf(topic);
//             if (topics[topic] && topics[topic].subscribed) {
//                 console.info('Topic '+topic+' already subscribed to (we cool)');
//                 if (pSubscribe !== -1)
//                     topics_to_subscribe.splice(pSubscribe, 1);
//                 continue;
//             }
//             if (pSubscribe === -1)
//                 topics_to_subscribe.push(topic);
//             let pUnsubscribe = topics_to_unsubscribe.indexOf(topic);
//             if (pUnsubscribe !== -1)
//                 topics_to_unsubscribe.splice(pUnsubscribe, 1);
//         } else {
//             let pUnsubscribe = topics_to_unsubscribe.indexOf(topic);
//             if (topics[topic] && !topics[topic].subscribed) {
//                 console.info('Topic '+topic+' already unsubscribed from (we cool)');
//                 if (pUnsubscribe !== -1)
//                     topics_to_unsubscribe.splice(pUnsubscribe, 1);
//                 continue;
//             }
//             if (pUnsubscribe === -1)
//                 topics_to_unsubscribe.push(topic);
//             let pSubscribe = topics_to_subscribe.indexOf(topic);
//             if (pSubscribe !== -1)
//                 topics_to_subscribe.splice(pSubscribe, 1);
//         }
//     }

//     let cum_topics_list = subscribe ? topics_to_subscribe : topics_to_unsubscribe;

//     if (!cum_topics_list.length) {
//         console.info('No topics to '+(subscribe?'subscribe to':'unsubscribe from')+' in SetTopicsReadSubscription (we cool)');
//         return;
//     }

//     if (subscribe) {

//         if (!pc || pc.signalingState != 'stable' || !pc.localDescription) {
//             if (pc && pc.connectionState == 'failed') {
//                 console.info('Cannot subscribe to topics, pc.connectionState='+pc.connectionState+'; pc.signalingState='+pc.signalingState+'; pc.localDescription='+pc.localDescription+'; waiting... '+cum_topics_list.join(', '));
//                 //connect will trigger this again
//                 return;
//             }

//             if (pc)
//                 console.info('Cannot subscribe to topics, pc.connectionState='+pc.connectionState+'; pc.signalingState='+pc.signalingState+'; pc.localDescription='+pc.localDescription+'; waiting... '+cum_topics_list.join(', '));
//             else
//                 console.info('Cannot subscribe to topics, pc=null; waiting... '+cum_topics_list.join(', '));

//             setTimeout(() => {
//                 SetTopicsReadSubscription(id_robot, [], subscribe) //all alteady in queues
//             }, 1000); //try again when stable
//             return;
//         }

//         _DoSetTopicsSubscription(id_robot, cum_topics_list, true)
//         topics_to_subscribe = [];

//     } else {
//         // unsubscribe, no need to renegotiate
//         _DoSetTopicsSubscription(id_robot, cum_topics_list, false)
//         topics_to_unsubscribe = [];
//     }
// }

// //assuming pc state is stable when subscribing to new topics here
// function _DoInitTopicsReadSubscription(id_robot, topics_list, subscribe) {



//     // if (!subscribe) {
//     return _DoSetTopicsSubscription(subscription_data, subscribe); //no need to negotiate
//     // }

//     // return pc.createOffer()
//     //     .then(function(offer) {
//     //         pc.setLocalDescription(offer);
//     //         //setup transievers for img topics
//     //         subscription_data['sdp_offer'] = pc.localDescription.sdp;
//     //     }).then(function() {
//     //         _DoSetTopicsSubscription(subscription_data, true);
//     //     });
// }

// function _DoSetTopicsSubscription(id_robot, topics_list, subscribe) {

//     console.log((subscribe ? 'Subscribing to read ' : 'Unsubscribing from reading ') + topics_list.join(', '));

//     let data = {
//         id_robot: id_robot,
//         topics: [],
//     };
//     for (let i = 0; i < topics_list.length; i++) {
//         if (!topics[topics_list[i]])
//             continue;
//         topics[topics_list[i]].subscribed = subscribe;
//         data.topics.push([ topics_list[i], subscribe ? 1 : 0 ]);
//     }

//     if (!data['topics'].length) {
//         return
//     }

//     return socket.emit('subcribe:read', data, (res) => {
//         if (!res || !res['success']) {
//             console.error('Read subscription err: ', res);
//             return;
//         }

//         if (subscribe) {

//             if (!res['offer_sdp']) {
//                 console.error('Read subscription err: no sdp offer received');
//                 return;
//             }

//             let robot_offer = new RTCSessionDescription({ sdp: res['offer_sdp'], type: 'offer' });
//             console.log('Setting robot offer, signalling state='+pc.signalingState, robot_offer);

//             // _HandleTopicSubscriptionReply(res); // preps video panel to be found when new media stream is added

//             pc.setRemoteDescription(robot_offer)
//             .then(() => {

//                 pc.createAnswer()
//                 .then((answer) => {
//                     pc.setLocalDescription(answer)
//                     .then(()=>{
//                         let answer_data = {
//                             id_robot: id_robot,
//                             sdp: answer.sdp,
//                         };
//                         socket.emit('sdp:answer', answer_data, (res_answer) => {
//                             if (!res_answer || !res_answer['success']) {
//                                 console.error('Error answering topic read subscription offer: ', res_answer);
//                                 return;
//                             }
//                         });
//                     })
//                 })
//             });

//         } else { // unsubscribe => no negotiation needed
//             // _HandleTopicSubscriptionReply(res);
//         }
//     });
// }

// function _HandleTopicSubscriptionReply(res) {

//     console.log('Handling topic subscription data: ', res);

//     for (let i = 0; i < res['subscribed'].length; i++) {

//         let topic = res['subscribed'][i][0];
//         let id = res['subscribed'][i][1];

//         if (!topics[topic]) {
//             console.warn('Topic '+topic+' not found in topics list', topics);
//             continue;
//         }

//         let is_image = topics[topic]['msg_types'][0] == 'sensor_msgs/msg/Image'

//         if (!is_image) { //subscribed data

//             if (topic_dcs[topic]) {
//                 continue;
//             }

//             console.log('Opening local read DC '+topic+' id='+id)
//             let dc = pc.createDataChannel(topic, {
//                 negotiated: true,
//                 ordered: false,
//                 maxRetransmits: 0,
//                 id:id
//             });

//             let Reader = window.Serialization.MessageReader;
//             let msg_type_class = find_message_type(topics[topic]['msg_types'][0], supported_msg_types)
//             let msg_reader = new Reader( [ msg_type_class ].concat(supported_msg_types) );

//             topic_dcs[topic] = dc;

//             dc.addEventListener('open', (ev)=> {
//                 console.warn('DC '+topic+' open', dc)
//             });
//             dc.addEventListener('close', (ev)=> {
//                 console.warn('DC '+topic+' close')
//                 delete topic_dcs[topic];
//             });
//             dc.addEventListener('error', (ev)=> {
//                 console.error('DC '+topic+' error', ev)
//                 delete topic_dcs[topic]
//             });
//             dc.addEventListener('message', (ev)=> {

//                 let rawData = ev.data; //arraybuffer
//                 let decoded = null;
//                 let raw_len = 0;
//                 let raw_type = ""

//                 if (rawData instanceof ArrayBuffer) {
//                     if (msg_reader != null) {
//                         let v = new DataView(rawData)
//                         decoded = msg_reader.readMessage(v);
//                     } else {
//                         decoded = buf2hex(rawData)
//                     }
//                     raw_len = rawData.byteLength;
//                     raw_type = 'ArrayBuffer';
//                 } else { //string
//                     decoded = rawData;
//                     raw_len = decoded.length;
//                     raw_type = 'String';
//                 }

//                 if (topic == '/robot_description') {
//                     console.warn('Got robot descripotion: ', decoded);
//                 }

//                 let panel = panels[topic];
//                 if (!panel) {
//                     console.error('panel not found for '+topic+' (data)')
//                     return;
//                 }

//                 if (!$('#update_panel_'+panel.n).is(':checked')) {
//                     // console.error('panel not updating '+topic+' (data)')
//                     return;
//                 }

//                 // console.log('panel '+topic+' has data', ev)

//                 panel.onData(ev, decoded, raw_type, raw_len);
//             });

//         } else { //image topic subscribed as video stream

//             console.log('Subscribing to video track '+topic+' id stream='+id+'; local streams:', media_streams);

//             topics[topic].id_stream = id;

//             let panel = panels[topic];
//             if (!panel) {
//                 console.error('Panel not found for '+topic);
//                 continue;
//             }

//             // if this is the first time stream is subscribed,
//             // panel will be found by 'track' event fires
//             panel.id_stream = id;

//             // otherwise we reuse existing panel
//             if (media_streams[id]) {
//                 console.log('Found stream for '+topic+' id='+id);
//                 document.getElementById('panel_video_'+panel.n).srcObject = media_streams[id];
//             }

//         }

//     }

//     if (res['unsubscribed']) {
//         for (let i = 0; i < res['unsubscribed'].length; i++) {

//             let id_topic = res['unsubscribed'][i][0];
//             let id = res['unsubscribed'][i][1];
//             let topic = topics[id_topic];

//             if (topic_dcs[id_topic]) {
//                 console.warn('Now closing local read DC '+id_topic);
//                 // topic_dcs[id_topic].close();
//                 // delete topic_dcs[id_topic];
//             }

//             // if (topic.id_stream && media_streams[topic.id_stream]) {

//             //     console.warn('Stopping media stream for '+id_topic);

//             //     // topic_video_tracks[topic].stop()

//             //     media_streams[topic.id_stream].getTracks().forEach(track => track.stop());
//             //     delete media_streams[topic.id_stream];

//             //     if (panels[id_topic]) {
//             //         console.log('Closing video panel for '+id_topic, document.getElementById('panel_video_'+panels[id_topic].n));
//             //         document.getElementById('panel_video_'+panels[id_topic].n).srcObject = undefined;
//             //     }
//             //     //pc.removeTrack(topic_transceivers[topic].receiver);
//             //     //topic_video_tracks[topic].stop();
//             //     //delete topic_video_tracks[topic];
//             // }
//         }
//     }

//     if (res['err']) {
//         for (let i = 0; i < res['err'].length; i++) {

//             let topic = res['err'][i][0];
//             let msg = res['err'][i][1];
//             console.info('Topic '+topic+' subscription err: '+msg);

//             if (topics[topic]) {
//                 topics[topic].subscribed = false;
//             }
//         }
//     }
// }


// let cameras_to_subscribe = []; // str cam
// let cameras_to_unsubscribe = []; // str cam
// function SetCameraSubscription(id_robot, camera_list, subscribe) {

//     for (let i = 0; i < camera_list.length; i++) {
//         let cam = camera_list[i];
//         if (subscribe) {
//             let pSubscribe = cameras_to_subscribe.indexOf(cam);
//             if (cameras[cam] && cameras[cam].subscribed) {
//                 console.info('Camera '+cam+' already subscribed to (we cool)');
//                 if (pSubscribe !== -1)
//                     cameras_to_subscribe.splice(pSubscribe, 1);
//                 continue;
//             }
//             if (pSubscribe === -1)
//                 cameras_to_subscribe.push(cam);
//             let pUnsubscribe = cameras_to_unsubscribe.indexOf(cam);
//             if (pUnsubscribe !== -1)
//                 cameras_to_unsubscribe.splice(pUnsubscribe, 1);
//         } else {
//             let pUnsubscribe = cameras_to_unsubscribe.indexOf(cam);
//             if (cameras[cam] && !cameras[cam].subscribed) {
//                 console.info('Camera '+cam+' already unsubscribed from (we cool)');
//                 if (pUnsubscribe !== -1)
//                     cameras_to_unsubscribe.splice(pUnsubscribe, 1);
//                 continue;
//             }
//             if (pUnsubscribe === -1)
//                 cameras_to_unsubscribe.push(cam);
//             let pSubscribe = cameras_to_subscribe.indexOf(cam);
//             if (pSubscribe !== -1)
//                 cameras_to_subscribe.splice(pSubscribe, 1);
//         }
//     }

//     let cum_cameras_list = subscribe ? cameras_to_subscribe : cameras_to_unsubscribe;

//     if (!cum_cameras_list.length) {
//         console.info('No cameras to '+(subscribe?'subscribe to':'unsubscribe from')+' in SetCameraSubscription (we cool)');
//         return;
//     }

//     if (subscribe) {

//         if (!pc || pc.signalingState != 'stable' || !pc.localDescription) {
//             if (pc && pc.connectionState == 'failed') {
//                 console.info('Cannot subscribe to cameras, pc.connectionState='+pc.connectionState+'; pc.signalingState='+pc.signalingState+'; pc.localDescription='+pc.localDescription+'; waiting... '+cum_cameras_list.join(', '));
//                 //connect will trigger this again
//                 return;
//             }

//             if (pc)
//                 console.info('Cannot subscribe to cameras, pc.connectionState='+pc.connectionState+'; pc.signalingState='+pc.signalingState+'; pc.localDescription='+pc.localDescription+'; waiting... '+cum_cameras_list.join(', '));
//             else
//                 console.info('Cannot subscribe to cameras, pc=null; waiting... '+cum_cameras_list.join(', '));

//             setTimeout(() => {
//                 SetCameraSubscription(id_robot, [], subscribe) //all alteady in queues
//             }, 1000); //try again when stable
//             return;
//         }

//         _DoSetCamerasSubscription(id_robot, cum_cameras_list, true)
//         cameras_to_subscribe = [];

//     } else {
//         // unsubscribe, no need to renegotiate
//         _DoSetCamerasSubscription(id_robot, cum_cameras_list, false)
//         cameras_to_unsubscribe = [];
//     }

// }

// assuming pc state is stable when subscribing to new cameras here
// function _DoInitCamerasSubscription() {

//     console.warn((subscribe ? 'Subscribing to caneras ' : 'Unsubscribing from cameras ') + cameras_list.join(', '));

//     let subscription_data = {
//         id_robot: id_robot,
//         cameras: [],
//     };
//     for (let i = 0; i < cameras_list.length; i++) {
//         if (!cameras[cameras_list[i]])
//             continue;
//         cameras[cameras_list[i]].subscribed = subscribe;
//         subscription_data.cameras.push([ cameras_list[i], subscribe ? 1 : 0 ]);
//     }

//     if (!subscription_data['cameras'].length) {
//         return
//     }

//     if (!subscribe) {
//         return _DoSetCamerasSubscription(subscription_data, false); //no need to negotiate
//     }

//     return pc.createOffer()
//         .then(function(offer) {
//             pc.setLocalDescription(offer);
//             //setup transievers for img topics
//             subscription_data['sdp_offer'] = pc.localDescription.sdp;
//         }).then(function() {
//             _DoSetCamerasSubscription(subscription_data, true);
//         });
// }

// function _DoSetCamerasSubscription(id_robot, cameras_list, subscribe) {

//     console.log((subscribe ? 'Subscribing to cameras ' : 'Unsubscribing from cameras ') + cameras_list.join(', '));

//     let data = {
//         id_robot: id_robot,
//         cameras: [],
//     };
//     for (let i = 0; i < cameras_list.length; i++) {
//         if (!cameras[cameras_list[i]])
//             continue;
//         cameras[cameras_list[i]].subscribed = subscribe;
//         data.cameras.push([ cameras_list[i], subscribe ? 1 : 0 ]);
//     }

//     if (!data['cameras'].length) {
//         return
//     }

//     return socket.emit('cameras:read', data, (res) => {
//         if (!res || !res['success']) {
//             console.error('Camera subscription err: ', res);
//             return;
//         }

//         if (subscribe) {

//             if (!res['offer_sdp']) {
//                 console.error('Read subscription err: no sdp offer received');
//                 return;
//             }
//             let robot_offer = new RTCSessionDescription({ sdp: res['offer_sdp'], type: 'offer' });
//             console.log('Setting robot offer, signalling state='+pc.signalingState, robot_offer);

//             pc.setRemoteDescription(robot_offer)
//             .then(() => {

//                 pc.createAnswer()
//                 .then((answer) => {
//                     pc.setLocalDescription(answer)
//                     .then(()=>{
//                         let answer_data = {
//                             id_robot: id_robot,
//                             sdp: answer.sdp,
//                         };
//                         socket.emit('sdp:answer', answer_data, (res_answer) => {
//                             if (!res_answer || !res_answer['success']) {
//                                 console.error('Error answering camera read subscription offer: ', res_answer);
//                                 return;
//                             }
//                             _HandleCamerasSubscriptionReply(res); // preps video panel to be found when new media stream is added
//                         });
//                     })
//                 })
//             });

//         } else { // unsubscribe => no negotiation needed
//             _HandleCamerasSubscriptionReply(res);
//         }
//     });
// }

// function _HandleCamerasSubscriptionReply(res) {

//     // console.log('Handling cameras subscription data: ', res);

//     for (let i = 0; i < res['subscribed'].length; i++) {

//         let id_cam = res['subscribed'][i][0];
//         let id_stream = res['subscribed'][i][1];

//         if (!cameras[id_cam]) {
//             console.warn('Camera '+id_cam+' not found in detected cameras list', cameras);
//             continue;
//         }

//         console.log('Subscribing to video track '+id_cam+' id stream='+id_stream+'; local media streams:', media_streams);

//         cameras[id_cam].id_stream = id_stream;

//         let panel = panels[id_cam];
//         if (!panel) {
//             console.error('Panel not found for '+id_cam);
//             continue;
//         }

//         // if this is the first time stream is subscribed,
//         // panel will be found by 'track' event fires
//         panel.id_stream = id_stream;

//         if (media_streams[id_stream]) {
//             console.log('Found stream for '+id_cam+' id='+id_stream);
//             document.getElementById('panel_video_'+panel.n).srcObject = media_streams[id_stream];
//         }
//     }

//     if (res['unsubscribed']) {
//         for (let i = 0; i < res['unsubscribed'].length; i++) {

//             let id_cam = res['unsubscribed'][i][0];
//             let id_stream = res['unsubscribed'][i][1];

//             if (topic_media_streams[id_cam]) {

//                 console.warn('Pausing video track for '+id_cam);

//                 const elements = document.querySelectorAll(`[srcObject="${topic_media_streams[id_cam].id}"]`);
//                 elements.forEach(element => {
//                     element.srcObject = null;
//                 });
//             }
//         }
//     }

//     if (res['err']) {
//         for (let i = 0; i < res['err'].length; i++) {

//             let id_cam = res['err'][i][0];
//             let msg = res['err'][i][1];
//             console.info('Camera '+id_cam+' subscription err: '+msg);

//             if (cameras[id_cam]) {
//                 cameras[id_cam].subscribed = false;
//             }
//         }
//     }

// }