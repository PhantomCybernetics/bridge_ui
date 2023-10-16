
import { lerpColor, linkifyURLs } from "./lib.js";
import * as THREE from 'three';
import { OrbitControls } from './three-addons/controls/OrbitControls.js';

// BATTERY VISUALIZATION
export function VideoWidget (panel, ignored) {

    if (!panel.display_widget) {

        console.log('making video el')
        $('#panel_widget_'+panel.n)
            .addClass('enabled video')
            .html('<video id="panel_video_'+panel.n+'" autoplay="true" playsinline="true" muted></video>' //muted allows video autoplay in chrome before user interactions
                + '<span id="video_stats_'+panel.n+'" class="video_stats"></span>'
                + '<span id="video_fps_'+panel.n+'" class="video_fps"></span>'
                ); //muted allows video autoplay in chrome before user interactions

        panel.display_widget = $('#panel_video_'+panel.n);

        panel.widget_menu_cb = function(panel) {

            //fps menu toggle
            $('<div class="menu_line"><label for="video_fps_cb_'+panel.n+'" class="video_fps_cb_label" id="video_fps_cb_label_'+panel.n+'">'
                +'<input type="checkbox" id="video_fps_cb_'+panel.n+'" checked class="video_fps_cb" title="Display video FPS"> FPS</label></div>'
                ).insertBefore($('#close_panel_link_'+panel.n).parent());

            $('#video_fps_cb_'+panel.n).change(function(ev) {
                if ($(this).prop('checked')) {
                    $('#video_fps_'+panel.n).addClass('enabled');
                } else {
                    $('#video_fps_'+panel.n).removeClass('enabled');
                }
            });

            $('#video_fps_'+panel.n).addClass('enabled'); //on by default

            //stats menu toggle
            $('<div class="menu_line"><label for="video_stats_cb_'+panel.n+'" class="video_stats_cb_label" id="video_stats_cb_label_'+panel.n+'">'
                +'<input type="checkbox" id="video_stats_cb_'+panel.n+'" class="video_stats_cb" title="Display video stats"> Stats for nerds</label></div>'
                ).insertBefore($('#close_panel_link_'+panel.n).parent());

            $('#video_stats_cb_'+panel.n).change(function(ev) {
                if ($(this).prop('checked')) {
                    $('#video_stats_'+panel.n).addClass('enabled');
                } else {
                    $('#video_stats_'+panel.n).removeClass('enabled');
                }
            });

        }
    }

}

// BATTERY VISUALIZATION
export function BatteryStateWidget (panel, decoded) {

    let minVoltage = 3.2*3; //todo load from robot
    let maxVoltage = 4.2*3;

    if (!panel.display_widget) {
        $('#panel_widget_'+panel.n).addClass('enabled battery');

        // let width = $('#panel_widget_'+panel.n).width();
        // let height = $('#panel_widget_'+panel.n).parent().innerHeight()-30;

        let options = {
            series: [ ],
            chart: {
                height: '100%',
                width: '100%',
                type: 'line',
                parentHeightOffset: 0,
                zoom: {
                    enabled: false
                },
                animations: {
                    enabled: false,
                    dynamicAnimation: {
                        enabled: false
                    }
                },
                selection: {
                    enabled: false
                },
                redrawOnParentResize: true,

            },
            // dataLabels: {
            //     enabled: true,
            //     offsetY: 20,
            //     formatter: function (value) {
            //         return value.toFixed(2) + " V";
            //     }
            // },
            stroke: {
                curve: 'straight',
            },
            // title: {
            //     text: 'Voltage over time',
            //     align: 'left'
            // },
            grid: {
                row: {
                    colors: ['#f3f3f3', 'transparent'], // takes an array which will be repeated on columns
                    opacity: 0.5
                },
            },
            xaxis: {
                //categories: panel.labels_trace,
                labels: {
                    show: false,
                }
            },
            yaxis: {
                min: minVoltage-0.2,
                max: maxVoltage+0.2,
                decimalsInFloat: 2,
                labels: {
                    formatter: function (value) {
                        return value.toFixed(2) + " V";
                    }
                },
            },
            annotations: {
                yaxis: [

                ]
            },
            tooltip: {
                enabled: false,
            }
        };

        if (maxVoltage > 0) {
            options.annotations.yaxis.push({
                y: maxVoltage,
                borderColor: '#00E396',
                label: {
                    borderColor: '#00E396',
                    style: {
                    color: '#fff',
                    background: '#00E396'
                    },
                    text: 'Full'
                }
                });
        }
        if (minVoltage > 0) {
            options.annotations.yaxis.push({
                y: minVoltage,
                borderColor: '#ff0000',
                label: {
                    borderColor: '#ff0000',
                    style: {
                    color: '#fff',
                    background: '#ff0000'
                    },
                    text: 'Empty'
                }
            });
        }

        panel.display_widget = new ApexCharts(document.querySelector('#panel_widget_'+panel.n), options);
        panel.display_widget.render();

        // panel.resize_event_handler = function () { }; //no need here
    }

    if (decoded != null) {

        panel.data_trace.push({
            x: decoded.header.stamp.nanosec / 1e9 + decoded.header.stamp.sec,
            y: decoded.voltage
        });

        if (panel.data_trace.length > panel.max_trace_length) {
            panel.data_trace.shift();
        }

        if (panel.display_widget) {
            panel.display_widget.updateSeries([ { data: panel.data_trace } ], false); //don't animate
        }
    }
}


// RANGE VISUALIZATION
export function RangeWidget (panel, decoded) {

    if (!panel.display_widget) {

        let options = {
            chart: {
                height: '100%',
                width: '100%',
                type: "radialBar",
                offsetY: 10,
                redrawOnParentResize: true,
            },
            series: [ ],
            colors: [ function(ev) {
                return lerpColor('#259FFB', '#ff0000', ev.value / 100.0);
            } ],

            plotOptions: {
                radialBar: {
                    hollow: {
                        margin: 15,
                        size: "70%"
                    },
                    track: {
                        show: true,
                    },
                    startAngle: -135,
                    endAngle: 135,
                    dataLabels: {
                        showOn: "always",
                        name: {
                            offsetY: -10,
                            show: true,
                            color: "#888",
                            fontSize: "13px"
                        },
                        value: {
                            color: "#111",
                            fontSize: "20px",
                            show: true,
                            formatter: function(val) {

                                if (val < 0.001)
                                    return "> "+panel.max_range.toFixed(1) +" m";
                                else
                                    return panel.data_trace[0].toFixed(3) + " m";
                            }
                        }
                    }
                }
            },
            stroke: {
                lineCap: "round",
            },
            labels: ["Distance"]
        };

        $('#panel_widget_'+panel.n).addClass('enabled range');

        panel.display_widget = new ApexCharts(document.querySelector('#panel_widget_'+panel.n), options);
        panel.display_widget.render();
    }

    if (decoded != null && panel.display_widget) {

        let range = decoded.range ? decoded.range : decoded.max_range;

        panel.max_range = decoded.max_range;
        panel.data_trace[0] = range; // val in m

        //display gage pos
        let gageVal = 100.0 - (Math.min(Math.max(range, 0), decoded.max_range) * 100.0 / decoded.max_range);

        panel.display_widget.updateSeries([ gageVal ], false);
    }
}


//laser scan visualization
export function LaserScanWidget (panel, decoded) {

    if (!panel.display_widget) {
        panel.max_trace_length = 5;
        $('#panel_widget_'+panel.n).addClass('enabled laser_scan');

        const canvas = $('#panel_widget_'+panel.n).html('<canvas id="panel_canvas_'+panel.n+'" width="'+panel.widget_width +'" height="'+panel.widget_height+'"></canvas>').find('canvas')[0];
        const ctx = canvas.getContext("2d");
        panel.display_widget = ctx;

        //const div = d3.selectAll();
        //console.log('d3 div', div)
        //panel.display_widget = new ApexCharts(document.querySelector('#panel_widget_'+panel.n), options);

        //panel.display_widget.render();
        // panel.zoom = 8.0;

        //zoom menu control
        panel.widget_menu_cb = () => {

            $('<div class="menu_line zoom_ctrl" id="zoom_ctrl_'+panel.n+'"><span class="minus">-</span><span class="val">Zoom: '+panel.zoom.toFixed(1)+'x</span><span class="plus">+</span></div>').insertAfter($('#panel_msg_types_'+panel.n).parent());
            $('#zoom_ctrl_'+panel.n+' .plus').click(function(ev) {
                panel.zoom +=1.0;
                $('#zoom_ctrl_'+panel.n+' .val').html('Zoom: '+panel.zoom.toFixed(1)+'x');
                panel.ui.update_url_hash();
            });
            $('#zoom_ctrl_'+panel.n+' .minus').click(function(ev) {
                if (panel.zoom - 1.0 <= 0) {
                    return;
                }
                panel.zoom -= 1.0;
                $('#zoom_ctrl_'+panel.n+' .val').html('Zoom: '+panel.zoom.toFixed(1)+'x');
                panel.ui.update_url_hash();
            });
        }

        // window.addEventListener('resize', () => {
        //     ResizeWidget(panel);
        //     RenderScan(panel);
        // });
        // $('#display_panel_source_'+panel.n).change(() => {
        //     ResizeWidget(panel);
        //     RenderScan(panel);
        // });
        panel.resize_event_handler = function () {
            // [ panel.widget_width, panel.widget_height ] = panel.getAvailableWidgetSize()
            LaserScanWidget_Render(panel);
        };
    }

    //console.log('widget', [panel.widget_width, panel.widget_height], frame);

    if (decoded) {
        let numSamples = decoded.ranges.length;
        let anglePerSample = 360.0 / numSamples;

        //panel.display_widget.fillStyle = "#ff0000";

        let scale = (panel.widget_height/2.0 - 20.0) / decoded.range_max;

        let newScanPts = [];
        for (let i = 0; i < numSamples; i++) {

            if (decoded.ranges[i] == null || decoded.ranges[i] > decoded.range_max || decoded.ranges[i] < decoded.range_min)
                continue;

            let pos = [
                0,
                decoded.ranges[i] * scale
            ]

            let arad = deg2rad(anglePerSample * i);
            let p = [
                Math.cos(arad)*pos[0] - Math.sin(arad)*pos[1],
                Math.sin(arad)*pos[0] + Math.cos(arad)*pos[1]
            ]

            newScanPts.push(p);
        }

        panel.data_trace.push(newScanPts);

        if (panel.data_trace.length > panel.max_trace_length) {
            panel.data_trace.shift();
        }

        panel.range_max = decoded.range_max; //save for later
        panel.scale = scale;

        LaserScanWidget_Render(panel);
    }

}

function LaserScanWidget_Render(panel) {

    let frame = [
        panel.widget_width/2.0,
        panel.widget_height/2.0
    ];

    let range = panel.range_max;

    //panel.display_widget.fillStyle = "#fff";
    panel.display_widget.clearRect(0, 0, panel.widget_width, panel.widget_height);

    for (let i = 0; i < panel.data_trace.length; i++) {
        let pts = panel.data_trace[i];

        for (let j = 0; j < pts.length; j++) {
            let p = [ pts[j][0]*panel.zoom, pts[j][1]*panel.zoom ]; //zoom applied here
            panel.display_widget.fillStyle = (i == panel.data_trace.length-1 ? "#ff0000" : "#aa0000");
            panel.display_widget.beginPath();
            panel.display_widget.arc(frame[0]+p[0], frame[1]-p[1], 1.5, 0, 2 * Math.PI);
            panel.display_widget.fill();
        }
    }

    //lines
    let range_int = Math.floor(range);
    for (let x = -range_int; x < range_int+1; x++) {
        panel.display_widget.beginPath();
        panel.display_widget.setLineDash(x == 0 ? [] : [panel.scale/20, panel.scale/10]);
        panel.display_widget.strokeStyle = x == 0 ? 'rgba(100,100,100,0.3)' : '#0c315480' ;

        //vertical
        //panel.widget_height
        let dd = Math.sqrt(Math.pow(range_int*panel.scale, 2) - Math.pow(x*panel.scale, 2))*panel.zoom;
        panel.display_widget.moveTo(frame[0] + (x*panel.scale)*panel.zoom, frame[1]-dd);
        panel.display_widget.lineTo(frame[0] + (x*panel.scale)*panel.zoom, frame[1]+dd);
        panel.display_widget.stroke();

        //horizontal
        panel.display_widget.moveTo(frame[0]-dd, frame[1]+(x*panel.scale)*panel.zoom);
        panel.display_widget.lineTo(frame[0]+dd, frame[1]+(x*panel.scale)*panel.zoom);
        panel.display_widget.stroke();
    }

    //frame dot on top
    panel.display_widget.fillStyle = "#26a0fc";
    panel.display_widget.beginPath();
    panel.display_widget.arc(frame[0], frame[1], 5, 0, 2 * Math.PI);
    panel.display_widget.fill();
}


//IMU VISUALIZATION
export function ImuWidget (panel, decoded) {

    if (!panel.display_widget) {

        $('#panel_widget_'+panel.n).addClass('enabled imu');
        // let q = decoded.orientation;
        [ panel.widget_width, panel.widget_height ] = panel.getAvailableWidgetSize()

        panel.scene = new THREE.Scene();
        panel.camera = new THREE.PerspectiveCamera( 75, panel.widget_width / panel.widget_height, 0.1, 1000 );

        panel.renderer = new THREE.WebGLRenderer({
            antialias : false,
        });
        panel.renderer.setSize( panel.widget_width, panel.widget_height );
        document.getElementById('panel_widget_'+panel.n).appendChild( panel.renderer.domElement );

        const geometry = new THREE.BoxGeometry( 1, 1, 1 );
        const material = new THREE.MeshStandardMaterial( { color: 0x00ff00 } );
        panel.cube = new THREE.Mesh( geometry, material );
        panel.scene.add( panel.cube );
        panel.cube.position.y = .5

        panel.cube.add(panel.camera)
        panel.camera.position.z = 2;
        panel.camera.position.x = 0;
        panel.camera.position.y = 1;
        panel.camera.lookAt(panel.cube.position);

        // const light = new THREE.AmbientLight( 0x404040 ); // soft white light
        // panel.scene.add( light );

        const directionalLight = new THREE.DirectionalLight( 0xffffff, 0.5 );
        panel.scene.add( directionalLight );
        directionalLight.position.set( 1, 2, 1 );
        directionalLight.lookAt(panel.cube.position);

        // const axesHelper = new THREE.AxesHelper( 5 );
        // panel.scene.add( axesHelper );

        const axesHelperCube = new THREE.AxesHelper( 5 );
        axesHelperCube.scale.set(1, 1, 1); //show z forward like in ROS
        panel.cube.add( axesHelperCube );

        const gridHelper = new THREE.GridHelper( 10, 10 );
        panel.scene.add( gridHelper );

        panel.display_widget = panel.renderer;

        // panel.animate();

        // window.addEventListener('resize', () => {
        //     ResizeWidget(panel);
        //     RenderImu(panel);
        // });
        // $('#display_panel_source_'+panel.n).change(() => {
        //     ResizeWidget(panel);
        //     RenderImu(panel);
        // });
        panel.resize_event_handler = function () {
            // ResizeWidget(panel);
            ImuWidget_Render(panel);
        };
    }

    if (panel.display_widget && decoded) {
        // LHS (ROS) => RHS (Three)
        panel.cube.quaternion.set(-decoded.orientation.y, decoded.orientation.z, -decoded.orientation.x, decoded.orientation.w);
        ImuWidget_Render(panel)
    }
}

//logger
function ImuWidget_Render(panel) {
    panel.renderer.render( panel.scene, panel.camera );
}

export function LogWidget (panel, decoded) {

    if (!$('#panel_widget_'+panel.n).hasClass('enabled')) {
        $('#panel_widget_'+panel.n).addClass('enabled log');
        panel.max_trace_length = 100;
        $('#panel_widget_'+panel.n).addClass('autoscroll')
        // console.log('AUTOSCROLL START')
        $('#panel_widget_'+panel.n).mouseenter(function() {
            $('#panel_widget_'+panel.n).removeClass('autoscroll');
            // console.log('AUTOSCROLL STOP')
            if (panel.animation != null) {
                //console.log('cancel animation ', panel.animation)
                $('#panel_widget_'+panel.n+'').stop();
                panel.animation = null
            }
        }).mouseleave(function() {
            $('#panel_widget_'+panel.n).addClass('autoscroll');
            // console.log('AUTOSCROLL START')
        });
    }

    if (decoded) {

        let line = '<div class="log_line">[<span class="name">'+decoded.name+'</span>] '
             + '<span class="time">'+decoded.stamp.sec+'.'+decoded.stamp.nanosec+'</span>: '
             + decoded.msg+'</div>';

        $('#panel_widget_'+panel.n).append(line);

        if ($('#panel_widget_'+panel.n+'.autoscroll .log_line').length > panel.max_trace_length) {
            $('#panel_widget_'+panel.n+'.autoscroll .log_line').first().remove();
        }

        if (panel.animation != null) {
            //console.log('cancel animation ', panel.animation)
            $('#panel_widget_'+panel.n+'').stop();
            panel.animation = null
        }

        panel.animation = $('#panel_widget_'+panel.n+'.autoscroll').animate({
            scrollTop: $('#panel_widget_'+panel.n).prop("scrollHeight")
        }, 300, 'linear', () => {
            panel.animation = null
        });
    }
}

// UTILS
function deg2rad(degrees)
{
  var pi = Math.PI;
  return degrees * (pi/180);
}

//IMU VISUALIZATION
export function URDFWidget (panel, decoded) {

    if (!panel.display_widget) {

        $('#panel_widget_'+panel.n).addClass('enabled imu');
        $('#panel_widget_'+panel.n).data('gs-no-move', 'yes');

        // let q = decoded.orientation;
        [ panel.widget_width, panel.widget_height ] = panel.getAvailableWidgetSize()

        panel.scene = new THREE.Scene();
        panel.camera = new THREE.PerspectiveCamera( 75, panel.widget_width / panel.widget_height, 0.1, 1000 );

        panel.renderer = new THREE.WebGLRenderer({
            antialias : false,
        });
        panel.renderer.setSize( panel.widget_width, panel.widget_height );
        document.getElementById('panel_widget_'+panel.n).appendChild( panel.renderer.domElement );

        // const geometry = new THREE.BoxGeometry( .1, .1, .1 );
        // const material = new THREE.MeshStandardMaterial( { color: 0x00ff00 } );
        panel.model = new THREE.Object3D()
        panel.scene.add( panel.model );
        panel.model.position.y = .5

        panel.model.add(panel.camera)
        panel.camera.position.z = 2;
        panel.camera.position.x = 0;
        panel.camera.position.y = 1;
        panel.scene.add(panel.camera)
        panel.camera.lookAt(panel.model.position);

        panel.controls = new OrbitControls( panel.camera, panel.renderer.domElement );
        panel.renderer.domElement.addEventListener( 'pointerdown', (ev) => {
            ev.preventDefault(); //stop from moving the panel
        } );
        panel.controls.update();

        // const light = new THREE.AmbientLight( 0x404040 ); // soft white light
        // panel.scene.add( light );

        const directionalLight = new THREE.DirectionalLight( 0xffffff, 0.5 );
        panel.scene.add( directionalLight );
        directionalLight.position.set( 1, 2, 1 );
        directionalLight.lookAt(panel.model.position);

        // const axesHelper = new THREE.AxesHelper( 5 );
        // panel.scene.add( axesHelper );

        // const axesHelperCube = new THREE.AxesHelper( 5 );
        // axesHelperCube.scale.set(1, 1, 1); //show z forward like in ROS
        // panel.model.add( axesHelperCube );

        const gridHelper = new THREE.GridHelper( 10, 10 );
        panel.scene.add( gridHelper );

        panel.display_widget = panel.renderer;

        // panel.animate();

        // window.addEventListener('resize', () => {
        //     ResizeWidget(panel);
        //     RenderImu(panel);
        // });
        // $('#display_panel_source_'+panel.n).change(() => {
        //     ResizeWidget(panel);
        //     RenderImu(panel);
        // });
        panel.resize_event_handler = function () {
            // ResizeWidget(panel);
            URDFWidget_Render(panel);
        };
    }

    if (panel.display_widget && decoded) {
        // LHS (ROS) => RHS (Three)
        // panel.cube.quaternion.set(-decoded.orientation.y, decoded.orientation.z, -decoded.orientation.x, decoded.orientation.w);
        URDFWidget_Render(panel)
    }
}

//logger
function URDFWidget_Render(panel) {
    panel.controls.update();
    panel.renderer.render( panel.scene, panel.camera );
    window.requestAnimationFrame((step)=>{
        URDFWidget_Render(panel);
    });
}