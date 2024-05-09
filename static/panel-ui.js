import { BatteryStateWidget } from '/static/widgets/battery.js';
import { OccupancyGrid } from '/static/widgets/occupacy-grid.js';
import { VideoWidget } from '/static/widgets/video.js';
import { RangeWidget } from '/static/widgets/range.js';
import { LaserScanWidget } from '/static/widgets/laser-scan.js';
import { ImuWidget } from '/static/widgets/imu.js';
import { LogWidget } from '/static/widgets/log.js';
import { GraphMenu } from '/static/graph-menu.js';
import { PointCloudWidget } from '/static/widgets/pointcloud.js';
import { ServiceCallInput_Empty, ServiceCallInput_Bool } from './input-widgets.js'
import { IsImageTopic, IsFastVideoTopic } from '/static/browser-client.js';

import { Panel } from "./panel.js";
import { isPortraitMode, isTouchDevice, isSafari, detectHWKeyboard } from "./lib.js";

export class PanelUI {

    panels = {};

    lastAP = null;
    lastESSID = null;

    // override or edit to customize topic panel defaults
    topic_widgets = {
        // '/robot_description' : { widget: URDFWidget, w:5, h:4 } ,
    };
    type_widgets = {
        'sensor_msgs/msg/BatteryState': { widget: BatteryStateWidget, w: 4, h: 2 },
        'sensor_msgs/msg/Range': { widget: RangeWidget, w: 1, h: 1 },
        'sensor_msgs/msg/LaserScan': { widget: LaserScanWidget, w: 7, h: 4 },
        'sensor_msgs/msg/Imu': { widget: ImuWidget, w: 2, h: 2 },
        'rcl_interfaces/msg/Log': { widget: LogWidget, w: 10, h: 2 },
        'sensor_msgs/msg/Image': { widget: VideoWidget, w: 5, h: 4 },
        'sensor_msgs/msg/CompressedImage': { widget: VideoWidget, w: 5, h: 4 },
        'ffmpeg_image_transport_msgs/msg/FFMPEGPacket': { widget: VideoWidget, w: 5, h: 4 },
        'video': { widget: VideoWidget, w: 5, h: 4 },
        'sensor_msgs/msg/PointCloud2': { widget: PointCloudWidget, w: 4, h: 4 },
        'nav_msgs/msg/OccupancyGrid': { widget: OccupancyGrid, w: 7, h: 4 },
    };
    widgets = {}; // custom and/or compound

    input_widgets = {
        'std_srvs/srv/Empty': ServiceCallInput_Empty,
        'std_srvs/srv/SetBool': ServiceCallInput_Bool
    }

    constructor(client, grid_cell_height, keyboard, gamepad) {
        this.client = client;
        this.client.ui = this;

        // document.querySelector("body").requestFullscreen();

        let GridStack = window.exports.GridStack;
        this.grid = GridStack.init({
            float: false,
            animate: true,
            cellHeight: grid_cell_height,
            handle: '.panel-title',
            columnOpts: {
                breakpoints: [{ w: 500, c: 1 }]
            }
        });

        this.panels = {}
        this.panel_menu_on = null; //touch only
        this.maximized_panel = null;

        this.keyboard = keyboard;
        this.gamepad = gamepad;
        this.gamepad.ui = this;

        this.latest_nodes = null;
        this.latest_cameras = null;

        this.last_pc_stats = null;

        let that = this;
        this.small_menu_width = 245;
        this.burger_menu_open_item = null;
        this.burger_menu_open = false;

        this.lastAP = null;
        this.lastESSID = null;

        this.graph_menu = new GraphMenu(this);

        this.num_services = 0;
        this.num_cameras = 0;
        this.num_docker_containers = 0;
        this.num_widgets = 0;

        client.on('introspection', (state) => {
            if (state) {
                $('#introspection_state').addClass('active').removeClass('inactive').attr('title', 'Introspection running...');
            } else {
                $('#introspection_state').addClass('inactive').removeClass('active').attr('title', 'Run introspection...');
            }
        });
        client.on('peer_disconnected', () => {
            $('#introspection_state').addClass('inactive').removeClass('active').attr('title', 'Run introspection...');

            that.set_dot_state(2, 'red', 'Robot disconnected from Cloud Bridge (Socket.io)');
            that.update_wifi_signal(-1);
            that.update_num_peers(-1);
            that.update_rtt(-1);
            // this.update_wifi_status();
            $('#trigger_wifi_scan').css('display', 'none');
            $('#robot_wifi_info').empty().css('display', 'none');
        });

        client.on('error', (error, msg) => {
            that.show_page_error(error, msg);
        });

        client.on('update', () => { // from socket

            if (client.name) {
                $('#robot_name .label').html(client.name);
                document.title = client.name + ' @ PHNTM bridge';
                that.save_last_robot_name();
            }

            $('#robot_info').html('<span class="label">Robot ID:</span> ' + client.id_robot + '<br>'
                + '<span class="label">Robot IP (public):</span> ' + (client.robot_online ? '<span class="online">' + client.ip.replace('::ffff:', '') + '</span>' : '<span class="offline">Offline</span>')
            );

            that.set_dot_state(1, client.robot_online ? 'green' : 'red', 'Robot ' + (client.robot_online ? 'conected to' : 'disconnected from') + ' Cloud Bridge (Socket.io)');
            if (!client.robot_online) {
                that.update_wifi_signal(-1);
                that.update_num_peers(-1);
                that.update_rtt(-1);
            }
            that.update_webrtc_status()
            that.update_layout(); // robot name length affects layout
        });

        client.on('socket_disconnect', () => {
            that.set_dot_state(1, client.robot_online ? 'green' : 'red', 'Robot ' + (client.robot_online ? 'conected to' : 'disconnected from') + ' Cloud Bridge (Socket.io)');
        });

        client.on('media_stream', (id_src, stream) => {
            console.warn('Client got a stream for ' + id_src, stream);

            let panel = that.panels[id_src];
            // console.log('id_panel: '+id_src+'; panel=', panel, that.panels)
            if (!panel)
                return;

            panel.id_stream = stream.id;
            console.log('Found video panel for new media stream ' + stream.id + ' src=' + id_src);
            if (document.getElementById('panel_video_' + panel.n)) {
                document.getElementById('panel_video_' + panel.n).srcObject = stream;
            }
        });


        let last_saved_name = this.load_last_robot_name();
        if (last_saved_name) {
            client.name = last_saved_name;
            $('#robot_name .label').html(client.name);
        }

        window.addEventListener("resize", (event) => {
            that.update_layout()
        });

        let battery_status_wrapper = (msg) => {
            that.update_battery_status(msg);
        }

        let iw_status_wrapper = (msg) => {
            that.update_wifi_status(msg);
        }

        this.battery_topic = null;
        this.iw_topic = null;
        this.battery_shown = this.load_last_robot_battery_shown();
        // display ui elements as last time to prevent them moving around too much during init
        if (this.battery_shown) {
            $('#battery-info').css('display', 'block');
        }
        if (this.load_last_robot_wifi_signal_shown()) {
            $('#signal-monitor').css('display', 'block');
            $('#network-details').css('display', '');
        }

        this.update_layout();

        client.on('ui_config', (robot_ui_config) => {
            //battery
            // let battery_shown = false;
            if (that.battery_topic && that.battery_topic != robot_ui_config['battery_topic']) {
                client.off(that.battery_topic, battery_status_wrapper);
                that.battery_topic = null;
            }
            if (robot_ui_config['battery_topic']) {
                that.battery_topic = robot_ui_config['battery_topic'];
                client.on(that.battery_topic, battery_status_wrapper);
                console.warn('battery topic is '+that.battery_topic)
                $('#battery-info').css('display', 'block');
                this.battery_shown = true;
            } else {
                $('#battery-info').css('display', 'none');
                this.battery_shown = false;
            }
            that.save_last_robot_battery_shown(this.battery_shown);

            //wifi status
            let wifi_shown = false;
            if (that.iw_topic && that.iw_topic != robot_ui_config['iw_monitor_topic']) {
                client.off(that.iw_topic, iw_status_wrapper);
                that.iw_topic = null;
                wifi_shown = false;
            }
            if (robot_ui_config['iw_monitor_topic']) {
                that.iw_topic = robot_ui_config['iw_monitor_topic'];
                client.on(that.iw_topic, iw_status_wrapper);
                $('#signal-monitor').css('display', 'block');
                $('#network-details').css('display', '');
                wifi_shown = true;
            } else {
                $('#signal-monitor').css('display', 'none');
                $('#network-details').css('display', 'none !important');
                wifi_shown = false;
            }
            that.save_last_robot_wifi_signal_shown(wifi_shown);
        });

        // we must open at least one webrtc channel to establish connection, 
        // so this subscribes every time
        // client.on('/iw_status', iw_status_wrapper);
       
        client.on('topics', (topics) => {
            that.init_panels(topics);
        });

        client.on('nodes', (nodes) => {
            that.services_menu_from_nodes(nodes);
            that.graph_from_nodes(nodes);
            this.latest_nodes = nodes;
            that.cameras_menu_from_nodes_and_devices();
        });

        client.on('cameras', (cameras) => {
            this.latest_cameras = cameras;
            that.cameras_menu_from_nodes_and_devices();
        });

        client.on('docker', (containers) => {

            $('#docker_list').empty();

            this.num_docker_containers = Object.keys(containers).length;

            if (this.num_docker_containers > 0) {
                $('#docker_controls').addClass('active');
            } else {
                $('#docker_controls').removeClass('active');
            }

            $('#docker_heading .full-w').html(this.num_docker_containers == 1 ? 'Container' : 'Containers');
            $('#docker_heading B').html(this.num_docker_containers);

            let i = 0;
            Object.values(containers).forEach((container) => {

                $('#docker_list').append('<div class="docker_cont ' + container.status + '" id="docker_cont_' + i + '" data-container="' + container.id + '">'
                    // + '<input type="checkbox" class="enabled" id="cb_cont_'+i+'"'
                    //+ (!topic.robotTubscribed?' disabled':'')
                    // + (panels[camera_data.id] ? ' checked': '' )
                    // + '/> '
                    + '<span '
                    + 'class="docker_cont_name" '
                    + '>'
                    + container.name
                    + '</span>' + ' [' + container.status + ']'
                    + '<div class="docker_btns">'
                    + '<button class="docker_run" title="Start"></button>'
                    + '<button class="docker_stop" title="Stop"></button>'
                    + '<button class="docker_restart" title="Restart"></button>'
                    + '</div>'
                    + '</div>'
                );

                $('#docker_cont_' + i + ' button.docker_run').click(function (event) {
                    if ($(this).hasClass('working'))
                        return;
                    $(this).addClass('working');
                    // console.log('Running '+cont_data.name);
                    let item = this;
                    client.docker_container_start(container.id, () => {
                        $(item).removeClass('working');
                    });
                });
                $('#docker_cont_' + i + ' button.docker_stop').click(function (event) {
                    if ($(this).hasClass('working'))
                        return;
                    $(this).addClass('working');
                    // console.log('Stopping '+cont_data.name);
                    let item = this;
                    client.docker_container_stop(container.id, () => {
                        $(item).removeClass('working');
                    });
                });
                $('#docker_cont_' + i + ' button.docker_restart').click(function (event) {
                    if ($(this).hasClass('working'))
                        return;
                    $(this).addClass('working');
                    // console.log('Restarting '+cont_data.name);
                    let item = this;
                    client.docker_container_restart(container.id, () => {
                        $(item).removeClass('working');
                    });
                });

                i++;
            });

        });

        client.on('peer_connected', () => {
            that.update_webrtc_status();
        })

        client.on('peer_disconnected', () => {
            that.update_webrtc_status();
        })

        // browser's Socket.io connection to the Cloud Bridge's server
        client.socket.on('connect', () => {
            $('#socketio_status').html('<span class="label">Cloud Bridge:</span> <span class="online">Connected (Socket.io)</span>');
            that.set_dot_state(0, 'green', 'This client is conected to Cloud Bridge (Socket.io)')
        });

        client.socket.on('disconnect', () => {
            $('#socketio_status').html('<span class="label">Cloud Bridge:</span> <span class="offline">Disconnected (Socket.io)</span>');
            that.set_dot_state(0, 'red', 'This client is disconnected from Cloud Bridge (Socket.io)')
        });

        setInterval(() => {
            if (client.pc) {
                client.pc.getStats(null).then((results) => {
                    that.last_pc_stats = results;
                    that.update_video_stats(results)
                }, err => console.log(err))
            }
        }, 1000);

        this.grid.on('added removed change', function (e, items) {
            // console.log('grid changed', items);
            if (items) {
                items.forEach(function (item) {
                    let id_src = $(item.el).find('.grid_panel').attr('data-source');
                    if (that.panels[id_src]) {
                        that.panels[id_src].auto_menu_position();
                        that.panels[id_src].onResize();
                        window.setTimeout(() => {
                            // console.warn('Delayed resize '+id_src);
                            if (that.panels[id_src])
                                that.panels[id_src].onResize();
                        }, 300); // animaiton duration
                    }
                });
            }
            that.update_url_hash();
        });

        this.grid.on('resizestart resize resizestop', function (e, el) {
            let id_source = $(el).find('.grid_panel').attr('data-source');
            that.panels[id_source].onResize();
        });

        $('#introspection_state').click((ev) => {

            let is_active = $('#introspection_state').hasClass('active')

            if (is_active) {
                $('#introspection_state').removeClass('active');
                $('#introspection_state').addClass('inactive');
            } else {
                $('#introspection_state').removeClass('inactive');
                $('#introspection_state').addClass('active');
            }

            client.socket.emit('introspection', { id_robot: client.id_robot, state: !is_active }, (res) => {
                if (!res || !res['success']) {
                    console.error('Introspection start err: ', res);
                    return;
                }
            });
        });

        $('#services_gamepad_mapping_toggle').click(function (event) {
            event.preventDefault();
            if (!$('#service_controls').hasClass('setting_shortcuts')) {
                $('#service_controls').addClass('setting_shortcuts');
                $('#services_gamepad_mapping_toggle').html('[cancel]');
            } else {
                $('#service_controls').removeClass('setting_shortcuts');
                $('#services_gamepad_mapping_toggle').html('[shortcuts]');
            }
        });


        $('#trigger_wifi_scan').click(() => {
            that.trigger_wifi_scan();
        });

        $('#graph_controls').on('mouseenter', (e) => {
            if ($('#graph_controls').hasClass('hover_waiting'))
                $('#graph_controls').removeClass('hover_waiting');
        });

        // hanburger menu handlers
        $('#menubar_hamburger, #menubar_hamburger_close').click(() => {
            that.set_burger_menu_state(!that.burger_menu_open);
        });
        $('#graph_controls_heading').click(() => {
            that.burger_menu_action('#graph_display');
        });
        $('#services_heading').click(() => {
            that.burger_menu_action('#service_list');
        });
        $('#cameras_heading').click(() => {
            that.burger_menu_action('#cameras_list');
        });
        $('#docker_heading').click(() => {
            that.burger_menu_action('#docker_list');
        });
        $('#widgets_heading').click(() => {
            that.burger_menu_action('#widget_list');
        });

        $('#fixed-header').on('mouseenter', (ev) => {
            $('BODY').addClass('menu-cancels-scroll');
        });

        $('#fixed-header').on('mouseleave', (ev) => {
            $('BODY').removeClass('menu-cancels-scroll');
        });

        $(window).on('scroll touchmove', { passive: false }, (ev) => {

            if (that.panel_menu_on) {

                if (that.menu_locked_scroll) {
                    ev.preventDefault();
                    ev.stopPropagation();
                    console.log('ignoring win scroll', ev);
                    // window.scrollTo(that.menu_locked_scroll.x, that.menu_locked_scroll.y);
                    return;
                }
                console.log('win scroll', ev);

                that.panel_menu_touch_toggle(); //off
            }
        });

        window.addEventListener('touchstart', (ev) => {
            // ev.preventDefault();
            // ev.stopPropagation();
        }, { passive: false });

        $(window).on('resize', () => {
            if (that.panel_menu_on) {
                if ($('#touch-ui-selector').hasClass('src_selection')) {
                    $('#touch-ui-dialog-underlay').trigger('click');
                }
                that.panel_menu_touch_toggle(); //off
            }
        });

        // prevent screen dimming on touch devices
        if (isTouchDevice()) { 

            // The wake lock sentinel.
            let wakeLock = null;

            // Function that attempts to request a screen wake lock.
            const requestWakeLock = async () => {
                try {
                    wakeLock = await navigator.wakeLock.request();
                    wakeLock.addEventListener('release', () => {
                        console.log('Screen Wake Lock released:', wakeLock.released);
                    });
                    console.log('Screen Wake Lock released:', wakeLock.released);
                } catch (err) {
                    console.error(`${err.name}, ${err.message}`);
                }
            };
            
            requestWakeLock();

            const handleVisibilityChange = async () => {
                if (wakeLock !== null && document.visibilityState === 'visible') {
                    await requestWakeLock();
                }
            };

            document.addEventListener('visibilitychange', handleVisibilityChange);
        }
    }

    panel_menu_autosize(panel) {
        let l = panel.menu_el.offset().left - panel.menu_content_el.width() - 15;
        // let max_w = window.innerWidth-20; // screen offset & padding
        let w_cont = panel.menu_content_el.innerWidth(); //includes padding
        if (l < 5) {
            l = 5;
        }
        panel.menu_content_el.css('height', ''); //unset
        let h_cont = panel.menu_content_el.innerHeight(); //includes padding
        let max_h = window.innerHeight - 50 - 10; // screne offset & padding
        let scrolls = h_cont > max_h;
        let h = scrolls ? max_h : h_cont - 10; // unset on fitting
        let t = panel.menu_el.offset().top - (h_cont / 2.0);
        if (panel.floating_menu_top !== null)
            t = panel.floating_menu_top;
        let min_top = $(window).scrollTop() + 25.0;
        let t0 = t;
        if (t < min_top) {
            console.log('over top');
            t = min_top;
        } else if (t + h + 10 > ($(window).scrollTop() + window.innerHeight - 10)) {
            console.log('over bottom');
            t = $(window).scrollTop() + window.innerHeight - 10 - h - 10;
        }
        // console.log('menu content='+h_cont+'; min-top='+min_top+'; scrolls='+scrolls+'; t='+t);

        panel.menu_content_el
            .css({
                left: l,
                top: t,
                height: scrolls ? h : '' //unset
            });
        panel.floating_menu_top = t;

        if (!scrolls)
            panel.menu_content_el.addClass('noscroll'); //stops body from scrolling through a non-scrolling menu
        else
            panel.menu_content_el.removeClass('noscroll');
    }

    panel_menu_touch_toggle(panel) {

        if (!panel && this.panel_menu_on) {
            panel = this.panel_menu_on;
        }

        if (this.panel_menu_on && this.panel_menu_on != panel) {
            this.panel_menu_touch_toggle(this.panel_menu_on) //turn off previous
        }

        if (!panel.menu_el.hasClass('open')) {
            this.panel_menu_autosize(panel);
            panel.menu_el.addClass('open');
            panel.menu_content_el.addClass('floating')
                .appendTo('BODY');

            this.panel_menu_on = panel;
            let that = this;
            // $('BODY').addClass('no-scroll');
            $('#menu-underlay')
                .css('display', 'block')
                .unbind()
                .on('click', (e) => {
                    if (that.menu_blocking_element) { // multiselect uses this to cancel src remove on touch ui
                        that.menu_blocking_element.trigger('cancel');
                        return;
                    }
                    //console.log('overlay clicked')
                    that.panel_menu_touch_toggle();
                });
        } else {
            panel.menu_el.removeClass('open');
            // $('BODY').removeClass('no-scroll');
            panel.menu_content_el
                .removeClass('floating')
                .removeClass('noscroll')
                .css({
                    left: '',
                    top: '',
                    height: ''
                })
                .appendTo(panel.menu_el);
            this.panel_menu_on = null;

            $('#menu-underlay')
                .css('display', '')
                .unbind();

            panel.floating_menu_top = null;
        }
    }

    show_page_error(error, msg) {
        // console.log('Showing error', msg, error);
        $('#page_message')
            .html(msg)
            .addClass('error');
        $('BODY')
            .addClass('has-page-message');
        this.showing_page_message = true;
    }

    set_burger_menu_state(open, animate = true) {

        let that = this;

        if (open) {

            if (!this.burger_menu_open) {
                $('#menubar_hamburger_close').text('Close');
                this.body_scroll_was_disabled_before_burger_menu = $('BODY').hasClass('no-scroll');
                $('BODY').addClass('no-scroll');
                $('#modal-underlay')
                    .css('display', 'block')
                    .unbind()
                    .on('click', (e) => {
                        //console.log('overlay clicked')
                        that.set_burger_menu_state(false, false);
                    });
            }

            this.set_burger_menu_width(this.small_menu_width, false); // no animation
            this.burger_menu_open = true;
            $('#menubar_content')
                .addClass('open');

            // move in menu bg
            $('#menubar_items')
                .css({
                    left: -this.small_menu_width + 'px' //starting pos off screen
                })
                .stop().animate({
                    left: '-16px' /* slide in from the left */
                }, 200);

        }
        else { // back > close

            if (this.burger_menu_open_item) {

                // console.log('closing burger menu open item '+this.burger_menu_open_item);

                $('#menubar_hamburger_close').text('Close');
                $('#menubar_scrollable').removeClass('open'); // disable menu scrolling
                $('#hamburger_menu_label')
                    .text('')
                    .removeClass('graph_controls')
                    .css('display', 'none');

                let open_el = $(this.burger_menu_open_item);

                if (animate) {
                    // move the opened item all the way out

                    this.set_burger_menu_width(this.small_menu_width, true); // animates
                    open_el.stop();

                    // bring menu items back
                    $('#menubar_items > DIV').stop().animate({
                        left: '0px'
                    }, 200, () => {
                        open_el
                            .removeClass('hamburger-open')
                            .css({
                                left: '', //unset
                                top: '',
                                width: ''
                            });
                    });

                    this.burger_menu_open_item = null;
                    return; //next call closes

                } else {

                    open_el.stop().css({
                        left: '', //unset
                        top: '',
                        width: ''
                    }).removeClass('hamburger-open')
                    $('#menubar_items > DIV').stop().css({
                        left: '0px',
                    });
                    this.burger_menu_open_item = null;

                }
            }

            //console.log('closing burger menu');

            // close -> roll out to the left
            this.burger_menu_open = false;
            if (animate) {
                $('#menubar_items')
                    .stop().animate({
                        left: -this.small_menu_width + 'px' //back to hidde
                    }, 200, () => {
                        $('#menubar_content').removeClass('open');
                        $('#menubar_items').css({
                            left: '' //unset 
                        });
                    });
            } else {
                $('#menubar_items')
                    .stop().css({
                        left: '', //unset 
                        width: ''
                    });
                $('#menubar_content').removeClass('open');
                $('#menubar_items > DIV').stop().css({
                    left: '', //unset
                });
            }

            if (!this.body_scroll_was_disabled_before_burger_menu) {
                $('BODY').removeClass('no-scroll');
            }
                
            $('#modal-underlay').css('display', 'none');
        }
    }


    burger_menu_action(what, h=null) {

        if (!$('BODY').hasClass('hamburger') || !this.burger_menu_open)
            return;

        let now_opening = this.burger_menu_open_item === null;
        // console.warn('Burger menu: '+what);
        this.burger_menu_open_item = what;
        // let small_menu_full_width = 255;

        $('#menubar_hamburger_close').text('Back');
        $('#menubar_scrollable').addClass('open'); // disable menu scrolling

        let el = $(what);
        let w_body = $('body').innerWidth();
        let min_cont_w = parseInt(el.data('min-width')); //?? +20; //+margin
        let max_cont_w = parseInt(el.data('max-width')); //?? +20; //+margin
        console.log(`setting burger menu w ${what} min_cont_w: ${min_cont_w} max_cont_w ${max_cont_w}`);

        let menu_w = min_cont_w;
        if (menu_w < 1) { //no min
            menu_w = this.small_menu_width;
        } else {
            menu_w += 20; // add padding to min-width
        }

        if (what == '#graph_display') {
            if (menu_w > w_body - 20) {
                this.graph_menu.set_narrow(true);
                menu_w = 320; //only topics are shown
            } else {
                this.graph_menu.set_narrow(false);
            }
        }

        if (menu_w > w_body - 200) {
            menu_w = w_body - 40; //maximize if close to edge
        }

        if (what == '#graph_display') {
            this.graph_menu.set_dimensions(menu_w, h); // h passed from update_layout is graph height
        }

        this.set_burger_menu_width(menu_w);

        let el_w = menu_w - 20;

        if (now_opening) {
            // console.log('Now opening '+what);

            // move menu items out to the left
            $('#menubar_items > DIV').stop().animate({
                left: -(this.small_menu_width + 20) + 'px'
            }, 200, () => {
                // finished
            });

            let label = '';
            switch (what) {
                case '#graph_display':
                    label = this.graph_menu.node_ids.length + ' Nodes / ' + this.graph_menu.topic_ids.length + ' Topics';
                    break;
                case '#service_list':
                    label = this.num_services + ' Services';
                    break;
                case '#cameras_list':
                    label = this.num_cameras + ' Cameras';
                    break;
                case '#docker_list':
                    label = this.num_docker_containers + ' Containers';
                    break;
                case '#widget_list':
                    label = this.num_widgets + ' Widgets';
                    break;
            }

            $('#hamburger_menu_label')
                .text(label)
                .addClass('graph_controls')
                .css('display', 'block');

            let t = ((el.parent().parent().children().index(el.parent())) * -(15 + 15 + 21 + 1));
            t += $('#menubar_scrollable').scrollTop();
            el.css({
                width: el_w + 'px', //10 is padding
                left: (this.small_menu_width + 20) + 'px', // top right of the parent (moving) item
                top: t + 'px'
                })
                .addClass('hamburger-open')
                .stop();
            // .animate({
            //     left: this.small_menu_full_width + 'px', // compensate for moving parent menu item
            //     width: el_w +'px' //10 is padding
            // });
        } else {
            // console.log('Now updating '+what);
            el.css({
                width: el_w + 'px' //10 is padding
            });
        }



        // // $('BODY').addClass('menu-overlay');
        // let w_body = screen.availWidth;
        // if (w_body > 820) {
        //     w_body = 820;
        // }

        // let h_body = screen.availHeight;
        // let that = this;
        // $('#menubar_items').animate({
        //     left: '-16px', /* left screen edge */
        //     width: (w_body-10)+'px',
        //     height: (h_body-61)+'px',
        //   }, 5100, function() {
        //     console.log('yo menubar_items!');
        //     // Animation complete.
        //   });
        // $('#menubar_items > DIV').animate({
        //     left: '-'+(small_menu_full_width)+'px',
        //   }, 5100, function() {
        //     that.hamburger_menu_full_width = true;
        //     console.log('yo menubar_items > DIV!');
        //     // Animation complete.
        //   });

        // this.menu_overlay_el = $(what);

        // this.menu_overlay_el.css({
        //     'width': (w_body-20)+'px',
        //     'left': (w_body+small_menu_full_width)+'px',
        //     'height': (h_body-76)+'px',
        //     'top': '0px',
        //     'display': 'block'
        // }).animate({
        //     left: (small_menu_full_width-5)+'px',
        //   }, 5100, function() {
        //     that.menu_overlay_el.addClass('menu-overlay');
        //     console.log('yo !');
        //     // Animation complete.
        //   });
    }

    set_burger_menu_width(content_width, animate = true) {

        // let requisted_min_width = min_content_width;

        // if (content_width <= this.small_menu_full_width)
        //     content_width = this.small_menu_full_width-10;
        // else {
        //     // min_content_width += 20;

        //     // let w_body = $('body').innerWidth(); 
        //     // if (min_content_width > w_body-200) {
        //     //     min_content_width = w_body-40; //expand to full if too close
        //     // } 
        // }

        let current_w = parseInt($('#menubar_items').css('width'));
        // console.warn('set_min_burger_menu_width: '+min_width+'; current='+current_w);

        let w = content_width;

        if (current_w != w) {
            if (animate) {
                $('#menubar_items')
                    .stop()
                    .animate({
                        width: w + 'px'
                    }, 200, () => {
                        // console.warn('set_min_burger_menu_width DONE');
                    });
            } else {
                $('#menubar_items')
                    .stop()
                    .css({
                        width: w + 'px'
                    });
            }
        }

        // return content_width;
    }

    init_panels(topics) {
        let that = this;
        let topic_ids = Object.keys(topics);
        topic_ids.forEach((id_topic) => {
            if (!that.panels[id_topic] || that.panels[id_topic].initiated)
                return;
            console.log(topics[id_topic]);
            let msg_type = topics[id_topic].msg_types[0];
            that.panels[id_topic].init(msg_type); //init w message type
        });
    }

    cameras_menu_from_nodes_and_devices() {

        $('#cameras_list').empty();

        let cameras = [];

        // built in camera devices
        if (this.latest_cameras) {
            Object.values(this.latest_cameras).forEach((cam) => {
                cameras.push({
                    src_id: cam.id,
                    msg_type: 'video'
                });
            });
        }
        // all other forwarded fast h.264 encoded topics for convenience
        if (this.latest_nodes) {
            let node_ids = Object.keys(this.latest_nodes);
            node_ids.forEach((id_node) => {
                let node = this.latest_nodes[id_node];
                if (node.publishers) {
                    let topic_ids = Object.keys(node.publishers);
                    topic_ids.forEach((id_topic) => {
                        let msg_type = node.publishers[id_topic].msg_types[0];
                        if (IsFastVideoTopic(msg_type)) {
                            cameras.push({
                                src_id: id_topic,
                                msg_type: msg_type
                            });
                        }
                    });
                }
            });
        }

        this.num_cameras = cameras.length;

        $('#cameras_heading .full-w').html(cameras.length == 1 ? 'Camera' : 'Cameras');
        $('#cameras_heading B').html(cameras.length);

        if (cameras.length > 0) {
            $('#camera_controls').addClass('active');
        } else {
            $('#camera_controls').removeClass('active');
        }

        for (let i = 0; i < cameras.length; i++) {
            let camera = cameras[i];

            $('#cameras_list').append('<div class="camera" data-src="' + camera.src_id + '" data-msg-type="' + camera.msg_type + '">'
                + '<input type="checkbox" class="enabled" id="cb_camera_' + i + '"'
                + (this.panels[camera.src_id] ? ' checked' : '')
                + '/> '
                + '<span '
                + 'class="camera" '
                + '>'
                + '<label for="cb_camera_' + i + '" class="prevent-select">' + camera.src_id + '</label>'
                + '</span>'
                + '</div>'
            );

            if (this.panels[camera.src_id]) {
                this.panels[camera.src_id].init(camera.msg_type);
            }
        }

        let that = this;
        $('#cameras_list INPUT.enabled:checkbox').change(function (event) {
            let id_cam = $(this).parent('DIV.camera').data('src');
            let msg_type = $(this).parent('DIV.camera').data('msg-type');
            let state = this.checked;

            let w = that.type_widgets[msg_type].w;
            let h = that.type_widgets[msg_type].h;

            that.toggle_panel(id_cam, msg_type, state, w, h);

            if (state && $('BODY').hasClass('hamburger')) {
                //close burger menu
                that.set_burger_menu_state(false, false);
            }
        });

    }

    graph_from_nodes(nodes) {

        this.graph_menu.update(nodes);

        $('#graph_nodes_label B').html(this.graph_menu.node_ids.length);
        $('#graph_topics_label B').html(this.graph_menu.topic_ids.length);
        $('#hamburger_menu_label.graph_controls').html(this.graph_menu.node_ids.length + ' Nodes / ' + this.graph_menu.topic_ids.length + ' Topics'); //update when open
        $('#graph_controls').addClass('active');
    }

    message_type_dialog(msg_type, onclose = null) {

        let msg_type_class = msg_type ? this.client.find_message_type(msg_type) : null;;
        let content = (msg_type_class ? JSON.stringify(msg_type_class, null, 2) : '<span class="error">Message type not loaded!</span>');

        if (!isTouchDevice()) {
            $('#msg_type-dialog')
                .html(content);
            $("#msg_type-dialog").dialog({
                resizable: true,
                draggable: true,
                height: 700,
                width: 700,
                top: 180,
                modal: true,
                title: msg_type,
                buttons: {
                    Okay: function () {
                        $(this).dialog("close");
                        if (onclose)
                            onclose();
                    },
                },
                close: function (event, ui) {
                    if (onclose)
                        onclose();
                }
            });
        } else {

            $('#touch-ui-dialog .title').html(msg_type);
            $('#touch-ui-dialog .content').html(content);
            let body_scroll_was_disabled = $('BODY').hasClass('no-scroll');
            $('BODY').addClass('no-scroll');
            $('#touch-ui-dialog').addClass('msg_type').css({
                display: 'block'
            });
            $('#touch-ui-dialog .content').scrollTop(0).scrollLeft(0);
            $('#close-touch-ui-dialog').unbind().click((e) => {
                $('#touch-ui-dialog').css('display', 'none').removeClass('msg_type');
                if (!body_scroll_was_disabled)
                    $('BODY').removeClass('no-scroll');
            });

        }

    }

    topic_selector_dialog(label, msg_type, exclude_topics, onselect, onclose=null, align_el = null) {

        let body_scroll_was_disabled = $('BODY').hasClass('no-scroll');
        $('BODY').addClass('no-scroll');

        // let d = !isTouchDevice() ? $('#topic-selector-dialog') : $('#touch-ui-selector .content');
        let d = $('#touch-ui-selector .content');
        let that = this;

        let offset = align_el.offset();
        let w_body = $('body').innerWidth();
        d.empty();

        const render_list = (discovered_topics) => {
            d.empty();

            let all_topics = Object.keys(discovered_topics);
            let some_match = false;
            all_topics.forEach((topic) => {

                if (exclude_topics && exclude_topics.length && exclude_topics.indexOf(topic) !== -1)
                    return;

                if (!that.client.discovered_topics[topic].msg_types || that.client.discovered_topics[topic].msg_types[0] != msg_type)
                    return;

                let l = $('<a href="#" class="topic-option">' + topic + '</a>');
                l.on('click', (e) => {
                    onselect(topic);
                    e.cancelBubble = true;               
                    $('#touch-ui-dialog-underlay').trigger('click'); //close
                    return false;
                });
                l.appendTo(d);
                some_match = true;
            });

            if (!some_match) {
                let l = $('<span class="empty">No matching topics foud</span>');
                
                $('#touch-ui-selector').unbind().click((ev) => {
                    $('#touch-ui-dialog-underlay').trigger('click');
                    ev.stopPropagation();
                });
                l.appendTo(d);
            }
            let w = d.parent().outerWidth();
            console.log('inner w='+w+'; offset='+offset.left+'; w_body='+w_body);
            if (w+offset.left+20 > w_body) {
                $('#touch-ui-selector').css('left', w_body-w-20);
            }
        }

        this.client.on('topics', render_list); // update on new topics
        
        
        $('#touch-ui-selector').addClass('src_selection').css({
            display: 'block',
            left: offset.left,
            top: (offset.top - $(window).scrollTop()) + 30
        });
        align_el.addClass('selecting');

        render_list(this.client.discovered_topics);

        $('#touch-ui-dialog-underlay')
            .css('display', 'block')
            .unbind()
            .on('click', (e) => { //close
                $('#touch-ui-selector').unbind()
                $('#touch-ui-selector').css('display', 'none').removeClass('src_selection');
                d.empty();
                if (!body_scroll_was_disabled)
                    $('BODY').removeClass('no-scroll');
                that.client.off('topics', render_list);
                $('#touch-ui-dialog-underlay').unbind().css('display', 'none');
                $('#close-touch-ui-dialog').unbind();
                align_el.removeClass('selecting');
                if (onclose) {
                    onclose();
                }
            });
    }


    add_widget(widget_class, conf) {
        this.widgets[widget_class.name] = {
            label: widget_class.label,
            class: widget_class,
        };
        this.widgets_menu();
    }

    widgets_menu() {
        $('#widget_list').empty();
        this.num_widgets = Object.keys(this.widgets).length;

        if (this.num_widgets > 0) {
            $('#widget_controls').addClass('active');
        } else {
            $('#widget_controls').removeClass('active');
        }
        //  $('#widgets_heading').html('<b>'+num_widgets+'</b> '+(num_widgets == 1 ? 'Widget' : 'Widgets'));

        let that = this;

        let i = 0;
        // let subscribe_cameras = [];
        Object.keys(this.widgets).forEach((widget_class) => {
            let w = that.widgets[widget_class];
            $('#widget_list').append('<div class="widget" data-class="' + widget_class + '">'
                + '<input type="checkbox" class="enabled" id="cb_widget_' + i + '"'
                //+ (!topic.robotTubscribed?' disabled':'')
                + (that.panels[widget_class] ? ' checked' : '')
                + '/> '
                + '<span '
                + 'class="custom_widget" '
                + '>'
                + '<label for="cb_widget_' + i + '" class="prevent-select">' + w.label + '</label>'
                + '</span>'
                + '</div>'
            );

            // let subscribe = $('#cb_camera_'+i).is(':checked');

            // if (that.panels[widget_class]) {
            //     that.panels[widget_class].init(widget_class);
            // }

            //if (!old_topics[topic.topic]) {

            // if (subscribe) {
            //     //console.warn('New topic: '+topic.topic+'; subscribe='+subscribe);
            //     subscribe_cameras.push(camera_data.id);
            // } else {
            //     //console.info('New topic: '+topic.topic+'; subscribe='+subscribe);
            // }

            //TogglePanel(topic.topic, true);
            //}

            i++;
        });


        // if (subscribe_cameras.length)
        //     SetCameraSubscription(id_robot, subscribe_cameras, true);

        $('#widget_list INPUT.enabled:checkbox').change(function (event) {
            let widget_class = $(this).parent('DIV.widget').data('class');
            let state = this.checked;

            let w = that.widgets[widget_class].class.default_width;
            let h = that.widgets[widget_class].class.default_height;

            that.toggle_panel(widget_class, widget_class, state, w, h);
            // client.SetCameraSubscription(id_robot, [ cam ], state);

            if (state && $('BODY').hasClass('hamburger')) {
                //close burger menu
                that.set_burger_menu_state(false, false);
            }
        });

    }

    services_menu_from_nodes(nodes) {

        $('#service_list').empty();
        this.num_services = 0;

        // let nodes_with_handled_ui = [];
        // let unhandled_nodes = [];

        Object.values(nodes).forEach((node) => {
            if (!node.services || !Object.keys(node.services).length)
                return;

            let node_content = $('<div class="node" data-node="' + node.node + '">' + node.node + '</div>');
            let service_contents = [];
            let service_ids = Object.keys(node.services);
            // let some_ui_handled = false;
            for (let i = 0; i < service_ids.length; i++) {

                let id_service = service_ids[i];
                let service = node.services[id_service];
                let msg_type = node.services[id_service].msg_types[0];

                if (['rcl_interfaces/srv/DescribeParameters',
                    'rcl_interfaces/srv/GetParameterTypes',
                    'rcl_interfaces/srv/GetParameters',
                    'rcl_interfaces/srv/ListParameters',
                    'rcl_interfaces/srv/SetParameters',
                    'rcl_interfaces/srv/SetParametersAtomically'
                ].includes(msg_type))
                    continue; // not rendering internals (?!)

                this.num_services++; // activates menu

                service.ui_handled = this.input_widgets[msg_type] != undefined;

                let service_content = $('<div class="service ' + (service.ui_handled ? 'handled' : 'nonhandled') + '" data-service="' + service.service + '" data-msg_type="' + service.msg_types[0] + '">'
                    + '<div '
                    + 'class="service_heading" '
                    + 'title="' + service.service + '\n' + msg_type + '"'
                    + '>'
                    + service.service
                    + '</div>'
                    + '<div class="service_input_type" id="service_input_type_' + i + '">' + msg_type + '</div>'
                    + '</div>');
                service_contents.push(service_content);
                // node_content.append(service_content);

                let service_input = $('<div class="service_input" id="service_input_' + i + '"></div>');


                if (service.ui_handled) {
                    this.input_widgets[msg_type](service_input, service, this.client);
                }

                service_content.append(service_input);
            }

            if (service_contents.length) {
                $('#service_list').append(node_content);
                for (let i = 0; i < service_contents.length; i++)
                    $('#service_list').append(service_contents[i]);
            }
        });

        $('#services_heading .full-w').html(this.num_services == 1 ? 'Service' : 'Services');
        $('#services_heading B').html(this.num_services);

        if (this.num_services > 0) {
            $('#service_controls').addClass('active');
        } else {
            $('#service_controls').removeClass('active');
        }

        if (this.gamepad)
            this.gamepad.MarkMappedServiceButtons();
    }

    trigger_wifi_scan() {
        if ($('#trigger_wifi_scan').hasClass('working'))
            return;
        $('#trigger_wifi_scan').addClass('working');
        this.client.wifi_scan(true, (res) => {
            $('#trigger_wifi_scan').removeClass('working');
        })
    }

    //widget_opts = {};

    toggle_panel(id_source, msg_type, state, w, h, x = null, y = null, src_visible = false, zoom) {
        let panel = this.panels[id_source];
        if (state) {
            if (!panel) {
                panel = new Panel(id_source, this, w, h, x, y, src_visible, zoom);
                panel.init(msg_type);

                if (isTouchDevice()) { // place new in editing state
                    this.grid.resizable(panel.grid_widget, true);
                    this.grid.movable(panel.grid_widget, true);
                    panel.editing = true;
                    $(panel.grid_widget).addClass('editing');
                }
            }
        } else if (panel) {
            panel.close();
        }
    }

    make_panel(id_source, w, h, x = null, y = null, src_visible = false, zoom, custom_url_vars) {
        if (this.panels[id_source])
            return this.panels[id_source];

        //msg type unknown here
        let panel = new Panel(id_source, this, w, h, x, y, src_visible, zoom, custom_url_vars);
        panel.init(null);

        if (isTouchDevice()) {
            this.grid.resizable(panel.grid_widget, false);
            this.grid.movable(panel.grid_widget, false);
        }

        // this.client.on(panel.id_source, panel._on_data_context_wrapper);

        this.panels[id_source] = panel;
        return panel;
    }

    update_video_stats(results) {
        let panel_ids = Object.keys(this.panels);
        let that = this;

        results.forEach(res => {
            if (res.type != 'inbound-rtp' || !res.trackIdentifier)
                return; //continue

            panel_ids.forEach((id_panel) => {
                let panel = that.panels[id_panel];
                if (panel.id_stream == res.trackIdentifier) {
                    let statsString = ''
                    statsString += `${res.timestamp}<br>`;
                    let fps = 0;
                    Object.keys(res).forEach(k => {
                        if (k == 'framesPerSecond') {
                            fps = res[k];
                        }
                        if (k !== 'timestamp' && k !== 'type' && k !== 'id') {
                            if (typeof res[k] === 'object') {
                                statsString += `${k}: ${JSON.stringify(res[k])}<br>`;
                            } else {
                                statsString += `${k}: ${res[k]}<br>`;
                            }
                        }
                    });
                    $('#video_stats_' + panel.n).html(statsString);
                    $('#video_fps_' + panel.n).html(fps + ' FPS');
                }
            });
        });
    }


    update_url_hash() {
        let hash = [];

        // console.log('Hash for :', $('#grid-stack').children('.grid-stack-item'));
        let that = this;
        // console.error('Grid nodes:', this.grid.engine.nodes);


        this.grid.engine.nodes.forEach((node) => {
            let widget = node.el;

            let wl = null;
            if (node.grid && node.grid.engine && node.grid.engine._layouts) {
                let len = node.grid.engine._layouts.length;
                if (len && node.grid.engine.column !== (len - 1)) {
                    let layout = node.grid.engine._layouts[len - 1];
                    wl = layout.find(l => l._id === node._id);
                }
            }

            let x, y, w, h;
            if (wl) { //use layout instead of node vals
                x = wl.x;
                y = wl.y;
                w = wl.w;
                h = node.h;
            } else {
                x = node.x;
                y = node.y;
                w = node.w;
                h = node.h;
            }

            let id_source = $(widget).find('.grid_panel').attr('data-source');

            let parts = [
                id_source,
                [x, y].join('x'),
                [w, h].join('x'),
            ];
            if (that.panels[id_source].src_visible)
                parts.push('src');
            if (that.panels[id_source].zoom !== undefined
                && that.panels[id_source].zoom !== null
                && that.panels[id_source].zoom != that.panels[id_source].default_zoom) {
                let z = Math.round(that.panels[id_source].zoom * 100) / 100;
                parts.push('z=' + z);
            }
            console.log('update_url_hash for ' + id_source + ': ', that.panels[id_source].display_widget);
            if (that.panels[id_source].display_widget && typeof that.panels[id_source].display_widget.getUrlHashParts !== 'undefined') {
                that.panels[id_source].display_widget.getUrlHashParts(parts);
            }

            hash.push(parts.join(':'));
        });

        if (hash.length > 0)
            window.location.hash = '' + hash.join(';');
        else //remove hash
            history.pushState("", document.title, window.location.pathname + window.location.search);
    }

    panels_from_url_hash(hash) {
        if (!hash.length) {
            return
        }

        hash = hash.substr(1);
        let hashArr = hash.split(';');
        let panels_to_make_sorted = [];
        let max_panel_x = 0;
        let max_panel_y = 0;
        for (let i = 0; i < hashArr.length; i++) {
            let src_vars = hashArr[i].trim();
            if (!src_vars)
                continue;
            src_vars = src_vars.split(':');
            let id_source = src_vars[0];
            let x = null; let y = null;
            let panelPos = src_vars[1];
            if (panelPos) {
                panelPos = panelPos.split('x');
                x = panelPos[0];
                y = panelPos[1];
            }
            let panelSize = src_vars[2];
            let w = 2; let h = 2;
            if (panelSize) {
                panelSize = panelSize.split('x');
                w = panelSize[0];
                h = panelSize[1];
            }

            //opional vars follow
            let src_on = false;
            let zoom = null;
            let custom_vars = [];
            for (let j = 3; j < src_vars.length; j++) {
                if (src_vars[j] == 'src') {
                    src_on = true;
                }
                else if (src_vars[j].indexOf('z=') === 0) {
                    zoom = parseFloat(src_vars[j].substr(2));
                    console.log('Found zoom for ' + id_source + ': ' + src_vars[j] + ' => ', zoom);
                } else {
                    custom_vars.push(src_vars[j].split('='));
                }
            }

            //let msg_type = null; //unknown atm
            //console.info('Opening panel for '+topic+'; src_on='+src_on);
            max_panel_x = Math.max(max_panel_x, x);
            max_panel_y = Math.max(max_panel_y, y);
            panels_to_make_sorted.push({
                'id_source': id_source,
                'w': w,
                'h': h,
                'x': x,
                'y': y,
                'src_on': src_on,
                'zoom': zoom,
                'custom_vars': custom_vars
            });
        }

        for (let y = 0; y <= max_panel_y; y++) {
            for (let x = 0; x <= max_panel_x; x++) {
                for (let j = 0; j < panels_to_make_sorted.length; j++) {
                    let p = panels_to_make_sorted[j];
                    if (p.x == x && p.y == y) {

                        this.make_panel(p.id_source, p.w, p.h, p.x, p.y, p.src_on, p.zoom, p.custom_vars)
                        if (this.widgets[p.id_source]) {
                            this.panels[p.id_source].init(p.id_source);
                        } // else if (this.widgets[id_source]) {
                        //     this.panels[id_source].init(id_source);
                        // }

                        break;
                    }
                }
            }
        }
        
        this.grid.engine.sortNodes();

        this.widgets_menu();
        return this.panels;
    }

    update_webrtc_status() {
        let state = null;
        let via_turn = null;
        let ip = 'n/a';
        let pc = this.client.pc;
        if (pc) {
            state = pc.connectionState
            // console.log('pc.sctp:', pc.sctp)
            if (pc.sctp && pc.sctp.transport && pc.sctp.transport.iceTransport) {
                // console.log('pc.sctp.transport:', pc.sctp.transport)
                // console.log('pc.sctp.transport.iceTransport:', pc.sctp.transport.iceTransport)
                let selectedPair = pc.sctp.transport.iceTransport.getSelectedCandidatePair()
                if (selectedPair && selectedPair.remote) {
                    via_turn = selectedPair.remote.type == 'relay' ? true : false;
                    ip = selectedPair.remote.address;
                }
            }
        }

        if (state != null)
            state = state.charAt(0).toUpperCase() + state.slice(1);
        else
            state = 'n/a'

        let wrtc_info = [ '<span class="label">WebRTC:</span> <span id="webrtc_status"></span>' ];
        if (via_turn)
            wrtc_info.push('<span class="label">TURN Server: </span> <span id="turn_ip" class="turn">'+ip+'</span>')
        else if (ip.indexOf('redacted') === -1 && ip != 'n/a')
            wrtc_info.push('<span class="label">IP: </span> <span id="robot_ip">'+ip+'</span>')

        $('#webrtc_info').html(wrtc_info.join('<br>'));


        if (state == 'Connected') {
            $('#webrtc_status').html('<span class="online">' + state + '</span>' + (via_turn ? ' <span class="turn">[TURN]</span>' : '<span class="online"> [P2P]</span>'));
            $('#trigger_wifi_scan').removeClass('working')
            if (via_turn)
                this.set_dot_state(2, 'yellow', 'WebRTC connected to robot (TURN)');
            else
                this.set_dot_state(2, 'green', 'WebRTC connected to robot (P2P)');
        } else if (state == 'Connecting') {
            $('#webrtc_status').html('<span class="connecting">' + state + '</span>');
            // $('#robot_wifi_info').addClass('offline')
            $('#trigger_wifi_scan').removeClass('working')
            this.set_dot_state(2, 'orange', 'WebRTC connecting...')
        } else {
            $('#webrtc_status').html('<span class="offline">' + state + '</span>');
            // $('#robot_wifi_info').addClass('offline')
            $('#trigger_wifi_scan').removeClass('working')
            this.set_dot_state(2, 'red', 'WebRTC ' + state)
        }

    }

    set_dot_state(no, color, label) {
        $('#dot-' + no)
            .removeClass('green')
            .removeClass('yellow')
            .removeClass('orange')
            .removeClass('red')
            .addClass(color)
            .attr('title', label);
    }

    update_wifi_signal(percent) {
        let el = $('#network-info #signal-monitor');
        if (percent < 0)
            el.attr('title', 'Robot disconnected');
        else
            el.attr('title', 'Robot\'s wi-fi signal quality: ' + Math.round(percent) + '%');

        el.removeClass('q25')
            .removeClass('q50')
            .removeClass('q75')
            .removeClass('q100');
        if (percent < 5)
            return;

        if (percent < 25)
            el.addClass('q25')
        else if (percent < 50)
            el.addClass('q50')
        else if (percent < 75)
            el.addClass('q75')
        else
            el.addClass('q100');
    }

    update_num_peers(num) {
        if (num > 1) { // only show on more peers
            $('#network-info-peers')
                .html(num)
                .attr('title', 'Multiple peers are connected to the robot')
                .css({
                    'display': 'block',
                    'background-color': (num == 1 ? 'white' : 'yellow')
                });
        } else {
            $('#network-info-peers')
                .empty()
                .css('display', 'none')
        }
    }

    update_rtt(rtt_sec) {

        if (rtt_sec > 0) {
            let rtt_ms = rtt_sec * 1000; //ms

            // TODO: customize these in config maybe?
            let rttc = '';
            if (rtt_ms > 100)
                rttc = 'red'
            else if (rtt_ms > 50)
                rttc = 'orange'
            else if (rtt_ms > 30)
                rttc = 'yellow'
            else
                rttc = 'lime'

            $('#network-info-rtt')
                .html(Math.floor(rtt_ms) + 'ms')
                .css({
                    'color': rttc,
                    'display': 'block'
                });
        } else {

            $('#network-info-rtt')
                .empty()
                .css('display', 'none');
        }

    }

    set_body_classes(enabled_classes) {

        let all_body_classes = ['full-width', 'narrow', 'narrower', 'hamburger', 'top-menu', 'touch-ui', 'desktop-ui', 'portrait', 'landscape'];
        let inactive_classes = [];

        for (let i = 0; i < all_body_classes.length; i++) {
            let c = all_body_classes[i];
            if (enabled_classes.indexOf(c) === -1)
                inactive_classes.push(c);
        }

        // console.log('Body removing/setting ', inactive_classes, enabled_classes);

        $('body')
            .removeClass(inactive_classes)
            .addClass(enabled_classes);
    }

    save_last_robot_name() {
        localStorage.setItem('last-robot-name:' + this.client.id_robot, this.client.name);
    }

    load_last_robot_name() {
        let name = localStorage.getItem('last-robot-name:' + this.client.id_robot);
        // console.log('Loaded keyboard driver for robot '+this.client.id_robot+':', dri);
        return name;
    }

    save_last_robot_battery_shown(val) {
        localStorage.setItem('last-robot-battery-shown:' + this.client.id_robot, val);
    }

    load_last_robot_battery_shown() {
        let val = localStorage.getItem('last-robot-battery-shown:' + this.client.id_robot) == 'true';
        return val;
    }

    save_last_robot_wifi_signal_shown(val) {
        localStorage.setItem('last-robot-wifi-shown:' + this.client.id_robot, val);
    }

    load_last_robot_wifi_signal_shown() {
        let val = localStorage.getItem('last-robot-wifi-shown:' + this.client.id_robot) == 'true';
        return val;
    }



    set_maximized_panel(max_panel) {
        this.maximized_panel = max_panel;
        let panel_ids = Object.keys(this.panels);
        let that = this;
        panel_ids.forEach((id_panel)=>{
            let p = that.panels[id_panel];
            if (p !== max_panel) {
                if (max_panel) {
                    if (!p.paused) {
                        p.pauseToggle();   
                    }
                } else {
                    if (p.paused) {
                        p.pauseToggle();
                    }
                        
                }
            }
        })
       
    }


    update_input_buttons() {

        $('#introspection_state').css('display', 'block'); //always

        let w_body = $('body').innerWidth();

        let min_only = w_body < 600 && isTouchDevice();
        
        if (detectHWKeyboard() && !min_only) { // TODO (??)
            $('#keyboard').css('display', 'block');
            // console.log('keyboard is on', navigator.keyboard, navigator.keyboard.getLayoutMap());
            // num_btns++;
        } else {
            $('#keyboard').css('display', 'none');
        }
        if (this.gamepad.gamepad && !min_only) {
            $('#gamepad').css('display', 'block');
            // num_btns++;
        } else {
            $('#gamepad').css('display', 'none');
        }

        if (isTouchDevice()) {
            $('#touch_ui').css('display', 'block');
            // num_btns++;
        } else {
            $('#touch_ui').css('display', 'none');
        }

        $('#fixed-right')
            .removeClass(['btns-4', 'btns-2'])
            .addClass('btns-'+(min_only ? 2 : 4));
    }

    //on resize, robot name update
    update_layout() {

        let k = 0;
        const full_menubar_w = 705-k;
        const narrow_menubar_w = 575-k;
        const narrower_menubar_w = 535-k;

        let w_body = $('body').innerWidth();

        this.update_input_buttons(); //changes #fixed-right
        let w_right = $('#fixed-right').innerWidth(); // right margin

        let label_el = $('h1 .label');
        label_el.removeClass('smaller');
        let w_left = $('#fixed-left').innerWidth()+60;

        let w_battery = this.battery_shown ? 4+23+5 : 0;

        let w_netinfo = $('#network-info').innerWidth();

        let max_label_w = w_body - w_right - w_netinfo - w_battery - 50;

        console.log(`max_label_w=${max_label_w}\nw_body=${w_body}\nw_right=${w_right}\nw_netinfo=${w_netinfo}\nw_battery=${w_battery}`)
        // w_left += w_netinfo;

        label_el.css({
            'max-width': max_label_w + 'px'
        });
        // let w_left_is_2rows = label_el.hasClass('hamburger');
        if (w_body < full_menubar_w + w_right + w_left) {
            label_el.addClass('smaller');
        }

        w_left = $('#fixed-left').innerWidth()+60;

        $('#fixed-center')
            .css({
                'margin-left': w_left + 'px',
                'margin-right': w_right + 'px',
            });

        let available_w_center = $('#fixed-center').innerWidth();

        let cls = [];
        let hamburger = false;

        let portrait = isPortraitMode();
        if (portrait) {
            cls.push('portrait');
        } else {
            cls.push('landscape');
        }

        if (portrait || available_w_center < narrower_menubar_w || isTouchDevice()) { // .narrower menubar
            cls.push('hamburger');
            hamburger = true;
            available_w_center = w_body; //uses full page width
            // if (this.menu_overlay_el) {

            // }
        } else if (available_w_center < narrow_menubar_w) { // .narrow menubar
            cls.push('narrower');
            cls.push('top-menu');
        }
        else if (available_w_center < full_menubar_w) { // full menubar
            cls.push('narrow');
            cls.push('top-menu');
        } else {
            cls.push('full-width');
            cls.push('top-menu');
        }


        if (hamburger) {

            let h = window.innerHeight; // does not work on mobils afari (adddress bar is not included)
            if (isTouchDevice() && isSafari()) {
                h = $(window).height(); // TODO: still not very good
            }
            if (this.showing_page_message) {
                h -= 50;
            }
            $('#menubar_items').css({
                height: (h-60) + 'px' // bg fills screenheight
            });
            $('#menubar_scrollable').css('height', h-74);
            //  let graph_w = $('#graph_display').innerWidth();
            
            $('#service_list').css('height', h-100);
            $('#cameras_list').css('height', h-100);
            $('#docker_list').css('height', h-100);
            $('#widget_list').css('height', h-100);
            $('#graph_display').css('height', h-100);

            if (this.burger_menu_open_item) {
                this.burger_menu_action(this.burger_menu_open_item, h-100); // only update
            }

            if (!$('BODY').hasClass('hamburger') || $('#bottom-links').hasClass('hidden')) { // switched
                $('#bottom-links')
                    .appendTo('#menubar_scrollable') // move to burger menu
                    .removeClass('hidden');
            }

            // if (!$('BODY').hasClass('hamburger') || $('#introspection_state').hasClass('hidden')) { // switched
            //     $('#introspection_state')
            //         .appendTo('#fixed-right') // move to the right icons
            //         .removeClass('hidden');
            // }

            if (h < 520) {
                $('#bottom-links').addClass('inline');
            } else {
                $('#bottom-links').removeClass('inline');
            }

            // if (window.innerHeight < 425) {
            //     console.log('hiding bottom links, window.innerHeight='+window.innerHeight)
            //     $('#bottom-links').css('display', 'none');
            // } else {
            //     console.log('showig bottom links, window.innerHeight='+window.innerHeight)
            //     $('#bottom-links').css('display', 'block');
            // }

        } else { // top menu on desktop

            if (this.burger_menu_open) {
                this.set_burger_menu_state(false, false); //no animations
            }
            $('#menubar_items').css({
                height: '' //unset
            });
            $('#menubar_scrollable').css('height', '');
            $('#graph_display').removeClass('narrow');
            $('#graph_display').css({
                'height': '',
                'width': '',
                'left': '',
                'top': ''
            }); // unset
            if (this.graph_menu) { // fixed desktop look
                this.graph_menu.set_dimensions(805, 600); // defaults
            }

            $('#service_list').css('height', ''); //unset
            $('#cameras_list').css('height', ''); //unset
            $('#docker_list').css('height', ''); //unset
            $('#widget_list').css('height', ''); //unset

            if ($('BODY').hasClass('hamburger') || $('#bottom-links').hasClass('hidden')) {
                $('#bottom-links')
                    .appendTo('body') // move to body
                    .css('display', 'block')
                    .removeClass(['inline', 'hidden']);
            }

            // if ($('BODY').hasClass('hamburger') || $('#introspection_state').hasClass('hidden')) { // switched
            //     $('#introspection_state')
            //         .appendTo('#menubar') // move center part
            //         .removeClass('hidden');
            // }
        };

        $('BODY.touch-ui #touch-ui-dialog .content').css({
            'height': (portrait ? window.innerHeight - 160 : window.innerHeight - 90) + 'px'
        });

        if (this.maximized_panel) {
            this.maximized_panel.maximize(true); //resize
        }

        //this.update_graph_menu_size();

        // let w_graph_menu = w_body+20;
        // let enough_room_for_graph = w_graph_menu > 800;

        // if (hb && !enough_room_for_graph && this.menu_overlay_el) {
        //     // let cnt = $('#menubar_content');
        //     $('#menubar_items')
        //         .addClass('narrow');
        //     // $('#service_list').appendTo(cnt);
        //     // $('#cameras_list').appendTo(cnt);
        //     // $('#docker_list').appendTo(cnt);
        //     // $('#widget_list').appendTo(cnt);
        // } else {
        //     $('#menubar_items')
        //         .removeClass('narrow');
        //     // $('#service_list').appendTo($('#service_controls'));
        //     // $('#cameras_list').appendTo($('#camera_controls'));
        //     // $('#docker_list').appendTo($('#docker_controls'));
        //     // $('#widget_list').appendTo($('#widget_controls'));
        // }

        // if (enough_room_for_graph) {
        //     w_graph_menu = 800;
        // }

        // if ($('#graph_display').hasClass('menu-overlay')) {
        //     $('#graph_display').css({
        //         'width': w_graph_menu+'px'
        //     });
        // } 

        // // if (this.hamburger_menu_full_width) {
        // if ($('#menubar_items').hasClass('narrow')) {
        //     $('#menubar_items').css({
        //         'width': (w_body+30)+'px', 
        //     });
        // } else {
        //     $('#menubar_items').css({
        //         'width': '', 
        //     });
        // }

        //}

        // let direct_editing_enabled = true;
        if (isTouchDevice()) {
            // direct_editing_enabled = false;
            cls.push('touch-ui');
        } else {
            cls.push('desktop-ui');
        }

        // Object.values(this.panels).forEach((p)=>{
        //     if (p.editing)
        //         return;

        //     if (direct_editing_enabled) 
        //         $(p.grid_widget).addClass('editing');
        //     else
        //         $(p.grid_widget).removeClass('editing');
        //     this.grid.resizable(p.grid_widget, direct_editing_enabled);
        //     this.grid.movable(p.grid_widget, direct_editing_enabled);
        // });

        this.set_body_classes(cls);

        $('body').addClass('initiated');

        let net_el = $('#network-info-wrapper #network-details');
        if (w_body < 600) {
            net_el
                .addClass('one-column')
                .css('width', w_body - 20); //-padding
        } else {
            net_el
                .removeClass('one-column')
                .css('width', ''); // unset
        }

        // console.log('Layout: body='+w_body+'; left='+w_left+'; right='+w_right+'; net='+w_netinfo+'; av center='+available_w_center);

        // console.info('body.width='+w+'px');
        // if (w < 1500)
        //     $('#robot_wifi_info').addClass('narrow_screen')
        // else
        //     $('#robot_wifi_info').removeClass('narrow_screen')

        Object.values(this.panels).forEach((panel) => {
            panel.onResize();
        });

    }

    update_battery_status(msg) {
        
        let topic = this.battery_topic;
        if (!topic || !this.client.topic_configs[topic])
            return;
        
        let voltage_min = this.client.topic_configs[topic].min_voltage;
        let voltage_max = this.client.topic_configs[topic].max_voltage;

        if (!this.battery_samples)
            this.battery_samples = [];
        this.battery_samples.push(msg.voltage);
        if (this.battery_samples.length > 5)
            this.battery_samples.shift();
        let v_smooth = 0;
        for (let i = 0; i < this.battery_samples.length; i++)
            v_smooth += this.battery_samples[i];
        v_smooth /= this.battery_samples.length;

        let range = voltage_max - voltage_min;
        let percent = Math.round(Math.max(0, Math.min(100, ((v_smooth-voltage_min)/range)*100.0)));

        if (percent > 75) {
            let c = 'lime';
            $('#battery-bar-0').css('background-color', c);
            $('#battery-bar-1').css('background-color', c);
            $('#battery-bar-2').css('background-color', c);
            $('#battery-bar-3').css('background-color', c);
        } else if (percent > 50) {
            let c = 'cyan';
            $('#battery-bar-0').css('background-color', 'transparent');
            $('#battery-bar-1').css('background-color', c);
            $('#battery-bar-2').css('background-color', c);
            $('#battery-bar-3').css('background-color', c);
        } else if (percent > 25) {
            let c = 'orange';
            $('#battery-bar-0').css('background-color', 'transparent');
            $('#battery-bar-1').css('background-color', 'transparent');
            $('#battery-bar-2').css('background-color', c);
            $('#battery-bar-3').css('background-color', c);
        } else {
            let c = 'red';
            $('#battery-bar-0').css('background-color', 'transparent');
            $('#battery-bar-1').css('background-color', 'transparent');
            $('#battery-bar-2').css('background-color', 'transparent');
            $('#battery-bar-3').css('background-color', c)
        }

        $('#battery-info').attr('title', `Battery at ${percent}%`)
        $('#battery-details').html(
            '<span class="label">Battery:</span> <span id="battery-percent">'+percent+'%</span><br>' +
            '<span class="label">Voltage:</span> <span id="battery-voltage">'+msg.voltage.toFixed(2)+'V</span>'
        )

    }

    update_wifi_status(msg) { // /iw_status in
        // console.warn('UpdateIWStatus', msg)

        let qPercent = (msg.quality / msg.quality_max) * 100.0;
        this.update_wifi_signal(qPercent);

        let apclass = '';
        if (this.lastAP != msg.access_point) {
            this.lastAP = msg.access_point;
            apclass = 'new'
        }

        let essidclass = ''
        if (this.lastESSID != msg.essid) {
            this.lastESSID = msg.essid;
            essidclass = 'new'
        }

        let html = '<div class="section-label">Connected to Wi-Fi</div>' +
            '<span class="label space">SSID:</span> ' + msg.essid + '<br>' +
            '<span class="label">Access Point:</span> ' + msg.access_point + '<br>' +
            '<span class="label">Frequency:</span> ' + (msg.frequency ? msg.frequency.toFixed(3) : null) + ' GHz<br>' +
            '<span class="label">BitRate:</span> ' + (msg.bit_rate ? msg.bit_rate.toFixed(1) : null) + ' Mb/s<br> ' +
            '<span class="label" title="' + msg.quality + '/' + msg.quality_max + '" style="cursor:help;">Quality:</span> ' + (qPercent).toFixed(0) + '%<br> ' +
            '<span class="label">Level:</span> ' + msg.level + '<br> ' +
            '<span class="label">Noise:</span> ' + msg.noise + ' '
            ;

        // $('#network-info-rtt').html();
        this.update_num_peers(msg.num_peers);

        // + ' ' + (msg.num_peers==1?'peer':'peers')

        $('#trigger_wifi_scan').css('display', msg.supports_scanning ? 'inline-block' : 'none')
        // $('#robot_wifi_info').removeClass('offline');

        // let selected_pair = this.client.pc.sctp.transport.iceTransport.getSelectedCandidatePair();
        // console.log('Selected pair', selected_pair);

        if (this.last_pc_stats) { // from timer
            // this.last_pc_stats.forEach((report) => {

            //     if (report.type == 'local-candidate') {
            //         console.warn('Report local-candidate', report);
            //         // return;
            //     }

            //     if (report.type == 'remote-candidate') {
            //         console.warn('Report remote-candidate', report);
            //         // return;
            //     }

            // });

            let min_rtt = -1;

            this.last_pc_stats.forEach((report) => {
                if (report.type != 'candidate-pair' || !report.nominated)
                    return;
                Object.keys(report).forEach((statName) => {
                    if (statName == 'currentRoundTripTime') {
                        if (min_rtt < 0 || report[statName] < min_rtt)
                            min_rtt = report[statName];
                    }
                });
            });

            if (min_rtt > 0) {
                this.update_rtt(min_rtt);
            }
        }

        $('#robot_wifi_info')
            .html(html)
            .css('display', 'block');
    }



}