import { lerpColor, linkifyURLs, lerp, deg2rad, rad2deg } from "../inc/lib.js";
import * as THREE from 'three';
import { STLLoader } from 'three/examples/jsm/loaders/STLLoader.js';
import { ColladaLoader } from 'three/examples/jsm/loaders/ColladaLoader.js';
import { OrbitControls } from 'orbit-controls';
import { LoadingManager } from 'three';
import URDFLoader from 'urdf-loader';
import { CSS2DRenderer, CSS2DObject } from 'css-2d-renderer';
import { MultiTopicSource } from "./inc/multitopic.js";
import { Vector3 } from "three";
import { Quaternion } from "three";

export class DescriptionTFWidget extends EventTarget {
    static label = 'Robot description (URFD) + Transforms';
    static default_width = 5;
    static default_height = 4;

    static L_VISUALS      = 1;
    static L_COLLIDERS    = 2;
    static L_JOINTS       = 3;
    static L_JOINT_LABELS = 4;
    static L_LINKS        = 5;
    static L_LINK_LABELS  = 6;
    static L_POSE_GRAPH   = 7;

    constructor(panel, start_loop=true) {
        super();

        this.panel = panel;

        // defaults overwritten by url params
        this.vars = {
            'render_collisions': true,
            'render_visuals': true,
            'render_labels': false,
            'render_links': true,
            'render_joints': true,
            'follow_target': true,
            'perspective_camera': true,
            'fix_robot_base': false, // robot will be fixed in place
            'render_pose_graph': false,
            'render_ground_plane': true,
            // pass through vars:
            '_focus_target': '',
            '_cam_position_offset': '',
        }

        this.pose_graph = [];
        this.pose_graph_size = 500; // keeps this many nodes in pg (TODO: to robot's config?)
    
        // this.latest_tf_stamps = {};
        // this.transforms_queue = [];
        this.smooth_transforms_queue = {};
        this.camera_pose_initialized = false; // first hard set pose, then smooth lerp
        this.camera_distance_initialized = false; // only once (if target autodetected)
        this.robot_pose_initialized = false; // true after 1st base transform
        this.camera_target_pose_initialized = false; // 1st cam to target will not lerp
        this.set_ortho_camera_zoom = -1; // if > 0, used on camera init
        // this.last_tf_stamps = {};

        let that = this;

        this.manager = new THREE.LoadingManager();
        this.manager.setURLModifier((url)=>{

            if (url.indexOf('http:/') === 0 || url.indexOf('https:/') === 0) 
                return url;

            if (url.indexOf('package:/') !== 0 && url.indexOf('file:/') !== 0) 
                return url;
                
            let url_fw = panel.ui.client.getBridgeFileUrl(url);
            console.log('URDF Loader requesting '+url+' > '+url_fw);
            return url_fw;

        });
        this.tex_loader = new THREE.TextureLoader(this.manager)
        this.urdf_loader = new URDFLoader(this.manager);
        this.urdf_loader.parseCollision = true;
        this.urdf_loader.packages = (targetPkg) => {
            return 'package://' + targetPkg; // put back the url scheme removed by URDFLoader 
        }
        this.robot_model = null; // urdf goes here
        this.robot = new THREE.Object3D();
    
        this.joint_markers = [];
        this.link_markers = [];

        this.urdf_loader.loadMeshCb = (path, manager, done_cb) => {

            console.log('Loaded mesh from '+path);

            if (/\.stl$/i.test(path)) {
    
                const loader = new STLLoader(manager);
                loader.load(path, (geom) => {
    
                    const stl_base_mat = new THREE.MeshPhongMaterial({
                        color: 0xffffff,
                        side: THREE.DoubleSide,
                        depthWrite: true,
                        transparent: false
                    });
                    
                    const mesh = new THREE.Mesh(geom, stl_base_mat);
                    done_cb(mesh);
                });
    
            } else if (/\.dae$/i.test(path)) {
    
                const loader = new ColladaLoader(manager);
                loader.load(path, dae => {
                    done_cb(dae.scene);
                });
    
            } else {
                console.error(`URDFLoader: Could not load model at ${path}.\nNo loader available`);
            }

        }
        this.manager.onLoad = () => {
            console.info('Robot URDF loaded', that.robot_model);
            if (that.robot_model) {
                if (!that.cleanURDFModel(that.robot_model)) { //clear after urdf fully loades; sets mats
                    console.error('Invalid URDF model imported; ignoring')
                    that.robot_model.removeFromParent();
                    that.robot_model = null;
                    that.renderDirty();
                    return;
                } 

                that.robot_model.visible = true; // hidden until clear
                that.makeRobotMarkers(); 
                that.renderDirty();
            }
        };
       
        $('#panel_widget_'+panel.n).addClass('enabled imu');
        $('#panel_widget_'+panel.n).data('gs-no-move', 'yes');
        
        // camera controls
        this.perspective_btn = $('<span class="panel-btn perspective-btn" title="Perspective"></span>')
        this.panel.panel_btns.append(this.perspective_btn);
        this.focus_btn = $('<span class="panel-btn focus-btn" title="Focus camera on selection"></span>')
        this.panel.panel_btns.append(this.focus_btn);
        this.labels_btn = $('<span class="panel-btn labels-btn" title="Display model labels"></span>')
        this.panel.panel_btns.append(this.labels_btn);

        [ panel.widget_width, panel.widget_height ] = panel.getAvailableWidgetSize()

        this.scene = new THREE.Scene();

        this.renderer = new THREE.WebGLRenderer({
            antialias : false,
            precision : 'highp' // TODO: med & low are really bad on some devices, there could be a switch for this in the menu
        });
        this.renderer.shadowMap.enabled = true;
        this.renderer.setSize(panel.widget_width, panel.widget_height);
        this.renderer.setPixelRatio(window.devicePixelRatio);
        document.getElementById('panel_widget_'+panel.n).appendChild(this.renderer.domElement);

        this.labelRenderer = new CSS2DRenderer();
        this.labelRenderer.setSize(panel.widget_width, panel.widget_height);
        this.labelRenderer.domElement.style.position = 'absolute';
        this.labelRenderer.domElement.style.top = '0px';
        document.getElementById('panel_widget_'+panel.n).appendChild(this.labelRenderer.domElement);

        this.camera_pos = new THREE.Vector3(1,.5,1);
        this.camera_target = null;
        this.camera_target_key = null;

        this.ros_space = new THREE.Object3D();
        this.ros_space.add(this.robot);
        this.robot.position.set(0,0,0);
        this.robot.quaternion.set(0,0,0,1);
        this.ros_space_default_rotation = new THREE.Quaternion().setFromEuler(new THREE.Euler(-Math.PI/2.0, 0.0, 0.0)); // ROS uses +z up
        this.scene.add(this.ros_space);
        this.ros_space.quaternion.copy(this.ros_space_default_rotation); 
        this.ros_space_offset_set = false; // will be set on 1st base tf data

        const light = new THREE.SpotLight( 0xffffff, 30.0, 0, Math.PI/10);
        light.castShadow = true; // default false
        this.scene.add(light);
        light.position.set(0, 5, 0); // will stay 5m above the model
        light.shadow.mapSize.width = 5 * 1024; // default
        light.shadow.mapSize.height = 5 * 1024; // default
        light.shadow.camera.near = 0.5; // default
        light.shadow.camera.far = 10; // default

        // light.shadow.bias= -0.002;
        this.renderer.shadowMap.type = THREE.PCFSoftShadowMap; // options are THREE.BasicShadowMap | THREE.PCFShadowMap | THREE.PCFSoftShadowMap
        this.light = light;

        const ambience = new THREE.AmbientLight( 0x606060 ); // soft white light
        this.scene.add(ambience);
        
        // this.makeMark(this.scene, 'SCENE ORIGIN', 0, 0, 2.0, true, 1.0);

        panel.resizeEventHandler = () => {
            that.labelRenderer.setSize(panel.widget_width, panel.widget_height);
            that.updateOrthoCameraAspect();
            that.renderDirty();
        };

        this.sources = new MultiTopicSource(this);
        this.sources.add('tf2_msgs/msg/TFMessage', 'Static transforms source', '/tf_static', 1, (topic, tf)=> { that.onTFData(topic, tf); });
        this.sources.add('tf2_msgs/msg/TFMessage', 'Real-time transforms source', '/tf', 1, (topic, tf) => { that.onTFData(topic, tf); });
        this.sources.add('std_msgs/msg/String', 'URDF description source', '/robot_description', 1, (topic, tf) => { that.onDescriptionData(topic, tf); });

        // this.set_camera_target_pos_on_description = null;
        // this.set_camera_target_offset_on_description = null;
        const camera_target_pos_geometry = new THREE.SphereGeometry( 0.01, 32, 16 ); 
        const camera_target_pos_material = new THREE.MeshBasicMaterial( { color: 0xff00ff } ); 
        this.camera_target_pos = new THREE.Mesh(camera_target_pos_geometry, camera_target_pos_material);
        this.camera_target_pos.position.set(0,0,0); // adjusted by url 
        // this.camera_target_pos.visible = false;
        this.scene.add(this.camera_target_pos);

        this.parseUrlParts(this.panel.custom_url_vars);

        // - url vars parsed here - //

        // cam follow action
        this.last_camera_url_update = Number.NEGATIVE_INFINITY;;
        if (this.vars.follow_target) {
            this.focus_btn.addClass('on');
        }
        this.focus_btn.click(function(ev) {
            that.vars.follow_target = !$(this).hasClass('on');
            if (that.vars.follow_target) {
                $(this).addClass('on');
                that.controls.enablePan = false;
            } else {
                $(this).removeClass('on');
                that.controls.enablePan = true;
                that.camera_target.getWorldPosition(that.camera_target_pos.position);
            }
            that.makeRobotMarkers();
            that.panel.ui.updateUrlHash(); 
            that.renderDirty();
        });

        // labels toggle 
        if (this.vars.render_labels) {
            this.labels_btn.addClass('on');
        }
        this.labels_btn.click(function(ev) {
            that.vars.render_labels = !$(this).hasClass('on');
            if (that.vars.render_labels) {
                $(this).addClass('on');
                if (that.vars.render_joints)
                    that.camera.layers.enable(DescriptionTFWidget.L_JOINT_LABELS);
                if (that.vars.render_links)
                    that.camera.layers.enable(DescriptionTFWidget.L_LINK_LABELS);
            } else {
                $(this).removeClass('on');
                that.camera.layers.disable(DescriptionTFWidget.L_JOINT_LABELS);
                that.camera.layers.disable(DescriptionTFWidget.L_LINK_LABELS);
            }
            // that.makeRobotMarkers();
            that.panel.ui.updateUrlHash(); 
            that.renderDirty();
        });

        // camera type
        if (this.vars.perspective_camera) {
            this.perspective_btn.addClass('on');
        } else {
            this.perspective_btn.removeClass('on');
        }
        this.perspective_btn.click(function(ev) {
            that.vars.perspective_camera = !$(this).hasClass('on');
            if (that.vars.perspective_camera) {
                $(this).addClass('on');
            } else {
                $(this).removeClass('on');
            }
            that.makeCamera();
            that.panel.ui.updateUrlHash(); 
            that.renderDirty();
        });

        // make camera (persp/orto) when type, pos and focus is determined
        this.makeCamera();
        this.camera.position.copy(this.camera_pos);

        this.camera.lookAt(this.camera_target_pos);

        this.controls = new OrbitControls(this.camera, this.labelRenderer.domElement);
        this.controls.enablePan = !this.vars.follow_target;
        this.renderer.domElement.addEventListener('pointerdown', (ev) => {
            ev.preventDefault(); // stop from moving the panel
        });
        this.controls.addEventListener('change', () => { this.controlsChanged(); });
        this.controls.addEventListener('end', () => {
            that.panel.ui.updateUrlHash(); // saves camera pos in url
        });
        this.controls_dirty = false;

        this.controls.target = this.camera_target_pos.position; // panning moves the target
        this.controls.update();

        light.lookAt(this.camera_target_pos);

        const plane_geometry = new THREE.PlaneGeometry( 100, 100 );

        // make ground plane
        this.tex_loader.load('/static/graph/tiles.png', (plane_tex) => {
            const plane_material = new THREE.MeshPhongMaterial( {color: 0xffffff, side: THREE.BackSide } );
            plane_tex.wrapS = THREE.RepeatWrapping;
            plane_tex.wrapT = THREE.RepeatWrapping;
            plane_tex.repeat.set(100, 100);
            plane_material.map = plane_tex;

            that.ground_plane = new THREE.Mesh(plane_geometry, plane_material);
            that.ground_plane.rotation.setFromVector3(new THREE.Vector3(Math.PI/2,0,0));
            that.ground_plane.position.set(0,0,0);
            that.ground_plane.receiveShadow = true;
            that.ground_plane.visible = that.vars.render_ground_plane;
            that.scene.add(that.ground_plane);
        });
        
        this.makeMark(this.ros_space, 'ROS_ORIGIN', 0, 0, 1.0, true, 1.0);
        // this.makeMark(this.robot, 'ROBOT ORIGIN', 0, 0, 1.0, true, 1.0);
    
        this.camera.layers.enableAll();
        if (!this.vars.render_visuals) this.camera.layers.disable(DescriptionTFWidget.L_VISUALS);
        if (!this.vars.render_collisions) this.camera.layers.disable(DescriptionTFWidget.L_COLLIDERS); //colliders off by default
        if (!this.vars.render_joints)  {
            this.camera.layers.disable(DescriptionTFWidget.L_JOINTS); 
            this.camera.layers.disable(DescriptionTFWidget.L_JOINT_LABELS); 
        }
        if (!this.vars.render_links) {
            this.camera.layers.disable(DescriptionTFWidget.L_LINKS);
            this.camera.layers.disable(DescriptionTFWidget.L_LINK_LABELS);

        }
        if (!this.vars.render_labels) {
            this.camera.layers.disable(DescriptionTFWidget.L_JOINT_LABELS);
            this.camera.layers.disable(DescriptionTFWidget.L_LINK_LABELS);
        }
        if (!this.vars.render_pose_graph) {
            this.camera.layers.disable(DescriptionTFWidget.L_POSE_GRAPH);
        }
        this.collider_mat = new THREE.MeshStandardMaterial({
            color: 0xffff00,
            emissive: 0xffff00,
            wireframe: true,
        });
        
        panel.widgetMenuCb = () => { that.setupMenu(); }
    
        if (start_loop) {
            this.rendering = true;
            this.renderDirty();
            requestAnimationFrame((t) => this.renderingLoop());  
        }
    }

    makeCamera() {

        let old_camera = this.camera;

        let aspect = this.panel.widget_width / this.panel.widget_height;

        if (this.vars.perspective_camera) {
            this.camera = new THREE.PerspectiveCamera(75,
                                                      aspect,
                                                      0.01, 1000);
        } else {
            const frustumSize = 1.0;
            this.camera = new THREE.OrthographicCamera(frustumSize * aspect / -2.0,
                                                       frustumSize * aspect / 2.0,
                                                       frustumSize / 2.0,
                                                       frustumSize / -2.0,
                                                       -1000, 1000); // negative near to prvent clipping while keeping the zoom functionality
            if (this.set_ortho_camera_zoom > 0) { //set zoom from url
                this.camera.zoom = this.set_ortho_camera_zoom;
                this.set_ortho_camera_zoom = -1;
            }
        }
        
        this.scene.add(this.camera);
        if (old_camera) {
            let old_type = old_camera.isOrthographicCamera ? 'ORTHO' : 'PERSP';
            console.log('Old '+old_type+' camera pos was ['+old_camera.position.x+';'+old_camera.position.y+';'+old_camera.position.z+']; zoom '+old_camera.zoom)

            this.camera.position.copy(old_camera.position);
            this.camera.quaternion.copy(old_camera.quaternion);
            this.camera.zoom = 1.0;

            if (this.vars.perspective_camera) {
                if (old_camera.isOrthographicCamera) { // compensate for ortho > persp
                    const targetDistance = this.camera.position.distanceTo(this.camera_target_pos.position);
                    const visibleHeight = (old_camera.top - old_camera.bottom) / old_camera.zoom;
                    const fovRadians = THREE.MathUtils.degToRad(this.camera.fov);
                    const requiredDistance = (visibleHeight / 2) / Math.tan(fovRadians / 2);
                    const moveDistance = requiredDistance - targetDistance;
                    const forwardVector = new THREE.Vector3(0, 0, 1).applyQuaternion(this.camera.quaternion);
                    this.camera.position.add(forwardVector.multiplyScalar(moveDistance));
                }
            } else {
                if (old_camera.isOrthographicCamera) {
                    this.camera.zoom = old_camera.zoom; // keep ortho zoom
                } else { // compensate for perp > ortho
                    const targetDistance = this.camera.position.distanceTo(this.camera_target_pos.position);
                    const fovRadians = THREE.MathUtils.degToRad(old_camera.fov);
                    const visibleHeight = 2 * Math.tan(fovRadians / 2) * targetDistance;
  
                    // Calculate the zoom factor for the orthographic camera
                    this.camera.zoom = 2 * this.camera.top / visibleHeight;
                }
            }
    
            old_camera.removeFromParent();
            this.camera.lookAt(this.camera_target_pos);
            this.camera.updateProjectionMatrix();
            this.camera.layers = old_camera.layers;
            this.controls.object = this.camera;
            this.controls.update();
        }
            
    }

    updateOrthoCameraAspect() {
        if (this.camera.isOrthographicCamera) {
            const aspect = this.panel.widget_width / this.panel.widget_height;
            const frustumSize = 1.0;
            this.camera.left = frustumSize * aspect / -2.0;
            this.camera.right = frustumSize * aspect / 2.0;
            this.camera.top = frustumSize / 2.0;
            this.camera.bottom = frustumSize / -2.0;
            this.camera.updateProjectionMatrix();
            this.renderDirty();
        }
    }

    setupMenu() {
       
        if (this.sources) {
            this.sources.setupMenu();
        }

        let that = this;

        $('<div class="menu_line"><label for="render_joints_'+this.panel.n+'"><input type="checkbox" '+(this.vars.render_joints?'checked':'')+' id="render_joints_'+this.panel.n+'" title="Render joints"> Render joints</label></div>')
            .insertBefore($('#close_panel_menu_'+this.panel.n));
        $('#render_joints_'+this.panel.n).change(function(ev) {
            that.vars.render_joints = $(this).prop('checked');
            if (that.vars.render_joints) {
                that.camera.layers.enable(DescriptionTFWidget.L_JOINTS);
                if (that.vars.render_labels)
                    that.camera.layers.enable(DescriptionTFWidget.L_JOINT_LABELS); //labels
            } else {
                that.camera.layers.disable(DescriptionTFWidget.L_JOINTS);
                that.camera.layers.disable(DescriptionTFWidget.L_JOINT_LABELS); //labels
            }
            if (that.robot_model)    
                that.makeRobotMarkers();
            // that.panel.ui.updateUrlHash(); 
            that.renderDirty();          
        });

        $('<div class="menu_line"><label for="render_links_'+this.panel.n+'"><input type="checkbox" '+(this.vars.render_links?'checked':'')+' id="render_links_'+this.panel.n+'" title="Render links"> Render links</label></div>')
            .insertBefore($('#close_panel_menu_'+this.panel.n));
        $('#render_links_'+this.panel.n).change(function(ev) {
            that.vars.render_links = $(this).prop('checked');
            if (that.vars.render_links) {
                that.camera.layers.enable(DescriptionTFWidget.L_LINKS);
                if (that.vars.render_labels)
                    that.camera.layers.enable(DescriptionTFWidget.L_LINK_LABELS); //labels
            } else {
                that.camera.layers.disable(DescriptionTFWidget.L_LINKS);
                that.camera.layers.disable(DescriptionTFWidget.L_LINK_LABELS); //labels
            }
            if (that.robot_model)    
                that.makeRobotMarkers();
            that.renderDirty();
        });

        // $('<div class="menu_line"><label for="render_labels_'+this.panel.n+'""><input type="checkbox" '+(this.vars.render_labels?'checked':'')+' id="render_labels_'+this.panel.n+'" title="Render labels"> Show labels</label></div>')
        //     .insertBefore($('#close_panel_menu_'+this.panel.n));
        // $('#render_labels_'+this.panel.n).change(function(ev) {
        //     that.vars.render_labels = $(this).prop('checked');
        //     if (that.vars.render_labels) {
        //         if ($('#render_joints_'+that.panel.n).prop('checked'))
        //             that.camera.layers.enable(DescriptionTFWidget.L_JOINT_LABELS);
        //         if ($('#render_links_'+that.panel.n).prop('checked'))
        //             that.camera.layers.enable(DescriptionTFWidget.L_LINK_LABELS);
        //     } else {
        //         that.camera.layers.disable(DescriptionTFWidget.L_JOINT_LABELS);
        //         that.camera.layers.disable(DescriptionTFWidget.L_LINK_LABELS);
        //     }
        //     // that.panel.ui.updateUrlHash();
        //     that.renderDirty();
        // });

        $('<div class="menu_line"><label for="render_visuals_'+this.panel.n+'""><input type="checkbox" '+(this.vars.render_visuals?'checked':'')+' id="render_visuals_'+this.panel.n+'" title="Render visuals"> Show visuals</label></div>')
            .insertBefore($('#close_panel_menu_'+this.panel.n));
        $('#render_visuals_'+this.panel.n).change(function(ev) {
            that.vars.render_visuals = $(this).prop('checked');
            if (that.vars.render_visuals)
                that.camera.layers.enable(DescriptionTFWidget.L_VISUALS);
            else
                that.camera.layers.disable(DescriptionTFWidget.L_VISUALS);
            // that.panel.ui.updateUrlHash();
            that.renderDirty();
        });

        $('<div class="menu_line"><label for="render_collisions_'+this.panel.n+'""><input type="checkbox" '+(this.vars.render_collisions?'checked':'')+' id="render_collisions_'+this.panel.n+'" title="Render collisions"> Show collisions</label></div>')
            .insertBefore($('#close_panel_menu_'+this.panel.n));
        $('#render_collisions_'+that.panel.n).change(function(ev) {
            that.vars.render_collisions = $(this).prop('checked');
            if (that.vars.render_collisions)
                that.camera.layers.enable(DescriptionTFWidget.L_COLLIDERS);
            else
                that.camera.layers.disable(DescriptionTFWidget.L_COLLIDERS);
            // that.panel.ui.updateUrlHash();
            that.renderDirty();
        });

        $('<div class="menu_line"><label for="fix_base_'+this.panel.n+'""><input type="checkbox" '+(this.vars.fix_robot_base?'checked':'')+' id="fix_base_'+this.panel.n+'" title="Fix robot base"> Fix robot base</label></div>')
            .insertBefore($('#close_panel_menu_'+this.panel.n));
        $('#fix_base_'+this.panel.n).change(function(ev) {
            that.vars.fix_robot_base = $(this).prop('checked');
            // that.panel.ui.updateUrlHash();
            that.renderDirty();
        });

        $('<div class="menu_line"><label for="render_pg_'+this.panel.n+'""><input type="checkbox" '+(this.vars.render_pose_graph?'checked':'')+' id="render_pg_'+this.panel.n+'" title="Render pose trace"> Render trace</label></div>')
            .insertBefore($('#close_panel_menu_'+this.panel.n));
        $('#render_pg_'+this.panel.n).change(function(ev) {
            that.vars.render_pose_graph = $(this).prop('checked');
            if (that.vars.render_pose_graph)
                that.camera.layers.enable(DescriptionTFWidget.L_POSE_GRAPH);
            else
                that.camera.layers.disable(DescriptionTFWidget.L_POSE_GRAPH);
            // that.panel.ui.updateUrlHash();
            that.renderDirty();
        });

        $('<div class="menu_line"><label for="render_grnd_'+this.panel.n+'""><input type="checkbox" '+(this.vars.render_ground_plane?'checked':'')+' id="render_grnd_'+this.panel.n+'" title="Render ground plane"> Ground plane</label></div>')
            .insertBefore($('#close_panel_menu_'+this.panel.n));
        $('#render_grnd_'+this.panel.n).change(function(ev) {
            that.vars.render_ground_plane = $(this).prop('checked');
            that.ground_plane.visible = that.vars.render_ground_plane;
            // that.panel.ui.updateUrlHash();
            that.renderDirty();
        });
    }

    onClose() {
        this.rendering = false; //kills the loop
        this.sources.close();
        
        this.controls.dispose();
        this.controls = null;
        this.scene.clear();
        this.scene = null;
        this.renderer.dispose();
        this.renderer = null;
    }

    getUrlHashParts (out_parts) {
        out_parts.push('f='+(this.vars.follow_target ? '1' : '0'));

        let focus_target = this.camera_target_key;
        if (!focus_target || !this.vars.follow_target) { // focus point in robot space
            let focus_pos = this.robot.worldToLocal(this.camera_target_pos.position.clone()); 
            focus_target = focus_pos.x.toFixed(3)+','+focus_pos.y.toFixed(3)+','+focus_pos.z.toFixed(3);
        }
        this.vars._focus_target = focus_target;
        out_parts.push('ft='+focus_target);

        out_parts.push('cam='+(this.vars.perspective_camera ? '1' : '0'));

        let cam_pos_robot_space = this.robot.worldToLocal(this.camera.position.clone());
        let cam_rot_angle = 0.0; // TODO ??!
        let val = cam_pos_robot_space.x.toFixed(3)+','+cam_pos_robot_space.y.toFixed(3)+','+cam_pos_robot_space.z.toFixed(3)+','+cam_rot_angle.toFixed(3);
        if (!this.vars.perspective_camera) //pass zoom for ortho cam as 4th val
            val += ','+this.camera.zoom.toFixed(3);
        this.vars._cam_position_offset = val;
        out_parts.push('cp='+val);
        
        out_parts.push('jnt='+(this.vars.render_joints ? '1' : '0'));
        out_parts.push('lnk='+(this.vars.render_links ? '1' : '0'));        
        out_parts.push('lbl='+(this.vars.render_labels ? '1' : '0'));
        out_parts.push('vis='+(this.vars.render_visuals ? '1' : '0'));
        out_parts.push('col='+(this.vars.render_collisions ? '1' : '0'));
        out_parts.push('fix='+(this.vars.fix_robot_base ? '1' : '0'));
        out_parts.push('pg='+(this.vars.render_pose_graph ? '1' : '0'));
        out_parts.push('grnd='+(this.vars.render_ground_plane ? '1' : '0'));
        this.sources.getUrlHashParts(out_parts);
    }

    parseUrlParts (custom_url_vars) {
        if (!custom_url_vars)
            return;
        custom_url_vars.forEach((kvp)=>{
            let arg = kvp[0];
            let val = kvp[1];
            // console.warn('DRF got ' + arg +" > "+val);
            switch (arg) {
                case 'f': this.vars.follow_target = parseInt(val) == 1; break;
                case 'ft': 
                    this.vars._focus_target = val;
                    if (val.indexOf(',') > 0) {
                        let coords = val.split(',');
                        let pos = new THREE.Vector3(parseFloat(coords[0]), parseFloat(coords[1]), parseFloat(coords[2]));
                        this.camera_target_pos.position.copy(this.robot.localToWorld(pos));
                        this.camera_target_key = null;
                    } else {
                        this.camera_target_key = val; // will be set on description
                    }
                    break;
                case 'cam': this.vars.perspective_camera = parseInt(val) == 1; break;
                case 'cp':
                    this.vars._cam_position_offset = val;
                    if (val.indexOf(',') > 0) {
                        let coords = val.split(',');
                        let pos = new THREE.Vector3(parseFloat(coords[0]), parseFloat(coords[1]), parseFloat(coords[2]));
                        let rot = parseFloat(coords[3]);
                        if (coords.length > 3)
                            this.set_ortho_camera_zoom = parseFloat(coords[4]);
                        this.camera_pos.copy(this.robot.localToWorld(pos));
                        this.camera_distance_initialized = true;
                        this.camera_pose_initialized = true;
                    }
                    break;
                case 'jnt': this.vars.render_joints = parseInt(val) == 1; break;
                case 'lnk': this.vars.render_links = parseInt(val) == 1; break;
                case 'lbl': this.vars.render_labels = parseInt(val) == 1; break;
                case 'vis': this.vars.render_visuals = parseInt(val) == 1; break;
                case 'col': this.vars.render_collisions = parseInt(val) == 1; break;
                case 'fix': this.vars.fix_robot_base = parseInt(val) == 1; break;
                case 'pg': this.vars.render_pose_graph = parseInt(val) == 1; break;
                case 'grnd': this.vars.render_ground_plane = parseInt(val) == 1; break;
            }
        });
        this.sources.parseUrlParts(custom_url_vars);
    }

    onDescriptionData (topic, desc) {

        if (this.panel.paused) // TODO: process last received data on unpause
            return;

        if (desc.data == this.last_processed_desc) {
            console.warn('Ignoring identical robot description from '+topic);
            return false;
        }

        if (this.robot_model) {
            this.onModelRemoved();
        }

        this.last_processed_desc = desc.data;

        console.warn('Parsing robot description...');
        this.robot_model = this.urdf_loader.parse(desc.data);
        this.robot_model.visible = false; // show when all loading is done and model cleaned
        this.robot.add(this.robot_model)
        this.robot_model.position.set(0,0,0); //reset pose, transform move with this.robot
        this.robot_model.quaternion.set(0,0,0,1);
        console.log('Robot desc received, model: ', this.robot_model);
        
        this.getAutofocusTarget();
        this.renderDirty();
    }

    cleanURDFModel(obj, lvl=0, inVisual=false, inCollider=false) {

        if (obj.isLight || obj.isScene || obj.isCamera) {
            return false;
        }   

        if (obj.isURDFVisual) {
            inVisual = true;
        } else if (obj.isURDFCollider) {
            inCollider = true;
        }

        // mesh visuals
        if (obj.isMesh && inVisual) {

            console.log('Visual:', obj);

            if (!obj.material) {
                obj.material = new THREE.MeshBasicMaterial({
                    color: 0xffffff,
                    side: THREE.DoubleSide,
                    depthWrite: true,
                    transparent: false
                });
                obj.material.needsUpdate = true;
            } else  {
                obj.material.depthWrite = true;
                obj.material.transparent = false;
                obj.material.side = THREE.FrontSide;
            }
            obj.material.needsUpdate = true;
            obj.castShadow = true;
            obj.receiveShadow = true;
            obj.renderOrder = -1;
            obj.layers.set(DescriptionTFWidget.L_VISUALS);
        
        // colliders
        } else if (obj.isMesh && inCollider) {

            console.log('Collider:', obj);

            obj.material = this.collider_mat;
            obj.scale.multiplyScalar(1.005); //make a bit bigger to avoid z-fighting
            obj.layers.set(DescriptionTFWidget.L_COLLIDERS);
        }

        if (obj.children && obj.children.length) {
            for (let i = 0; i < obj.children.length; i++) {
                let ch = obj.children[i];
                let res = this.cleanURDFModel(ch, lvl+1, inVisual, inCollider); // recursion
                if (!res) {
                    obj.remove(ch);
                    i--;
                }
            }
        }

        return true;
    }


    onModelRemoved() {
        console.warn('Removing robot model, clearing ros_space')
        // this.ros_space.clear(); // this.ros_space.remove(this.robot);
        // this.markRosOrigin(); //removed, put back
        this.robot_model.removeFromParent() // not changing or moving this.robot
        this.robot_model = null;
        this.last_processed_desc = null;
        while (this.labelRenderer.domElement.children.length > 0) {
            this.labelRenderer.domElement.removeChild(this.labelRenderer.domElement.children[0]); 
        }
    }

    getAutofocusTarget() {
        
        let robot = this.robot_model;

        if (this.camera_target_key && robot.joints[this.camera_target_key]) {
            this.setCameraTarget(robot.joints[this.camera_target_key], this.camera_target_key, false);
            return;
        }
        if (this.camera_target_key && robot.links[this.camera_target_key]) {
            this.setCameraTarget(robot.links[this.camera_target_key], this.camera_target_key, false);
            return;
        }

        let wp = new Vector3();
        let farthest_pt_dist = 0;
        let joints_avg = new Vector3(0,0,0);
        let joints_num = 0;
        let focus_joint = null;
        let focus_joint_key = null;

        // find the joint closest to avg center and set as target
        // also find the farthest for initial camera distance
        Object.keys(robot.joints).forEach((key)=>{
            robot.joints[key].getWorldPosition(wp);
            let wp_magnitude = wp.length();
            if (wp_magnitude > farthest_pt_dist)
                farthest_pt_dist = wp_magnitude;
            joints_avg.add(wp);
            joints_num++;
        });
        if (joints_num) {
            joints_avg.divideScalar(joints_num);
            let closest_joint_dist = Number.POSITIVE_INFINITY;
            Object.keys(robot.joints).forEach((key)=>{
                robot.joints[key].getWorldPosition(wp);
                let d = wp.distanceTo(joints_avg);
                if (d < closest_joint_dist) {
                    closest_joint_dist = d;
                    focus_joint = robot.joints[key];
                    focus_joint_key = key;
                }        
            });
        }
        Object.keys(robot.links).forEach((key)=>{
            robot.links[key].getWorldPosition(wp);
            let wp_magnitude = wp.length();
            if (wp_magnitude > farthest_pt_dist)
                farthest_pt_dist = wp_magnitude;
        });
        Object.keys(robot.frames).forEach((key)=>{
            robot.frames[key].getWorldPosition(wp);
            let wp_magnitude = wp.length();
            if (wp_magnitude > farthest_pt_dist)
                farthest_pt_dist = wp_magnitude;
        });

        // was this even needed? should pick the outermost frame of the model tho
        // if (robot.links['base_footprint']) {
        //     let v = new THREE.Vector3();
        //     robot.links['base_footprint'].getWorldPosition(v);
        //     this.ros_space.position.copy(v.negate());
        // }

        if (focus_joint && focus_joint_key)
            this.setCameraTarget(focus_joint, focus_joint_key, false);

        if (this.vars.follow_target && !this.camera_distance_initialized) {
            this.camera_distance_initialized = true;
            let initial_dist = farthest_pt_dist * 3.0; // initial distance proportional to model size
            this.camera_pos.normalize().multiplyScalar(initial_dist);
            this.camera.position.copy(this.camera_pos);
        }
    }

    setCameraTarget(new_target, new_target_key, force_follow=false) {

        console.log('Setting cam target to: '+new_target_key);
        this.camera_target = new_target;
        this.camera_target_key = new_target_key;

        if (!this.vars.follow_target && force_follow) {
            this.vars.follow_target = true;
            this.focus_btn.addClass('on');
        }

        if (force_follow) //only refresh on click
            this.panel.ui.updateUrlHash();

        this.makeRobotMarkers();
    }

    makeRobotMarkers() {

        this.joint_markers.forEach((m)=>{
            if (m.axis_el)
                m.axis_el.removeFromParent();
            if (m.label_el)
                m.label_el.removeFromParent()
        });
        this.joint_markers = [];

        this.link_markers.forEach((m)=>{
            if (m.axis_el)
                m.axis_el.removeFromParent();
            if (m.label_el)
                m.label_el.removeFromParent()
        });
        this.link_markers = [];

        let h_center = !this.vars.render_joints || !this.vars.render_links; // when rendering only one type, make it centered
        if (this.vars.render_joints)
            this.joint_markers = this.makeMarkerGroup(this.robot_model.joints, DescriptionTFWidget.L_JOINTS, DescriptionTFWidget.L_JOINT_LABELS, h_center);
        if (this.vars.render_links)
            this.link_markers = this.makeMarkerGroup(this.robot_model.links, DescriptionTFWidget.L_LINKS, DescriptionTFWidget.L_LINK_LABELS, h_center);
    }

    makeMarkerGroup(frames, layer_axes, layer_labels, h_center) {
        let that = this; 

        let markers = [];
        Object.keys(frames).forEach((key)=>{
            // robot.joints[key]
            let wp = new THREE.Vector3();
            frames[key].getWorldPosition(wp);
            markers.push({
                key: key,
                pos: wp,
                axis_el: null,
                label_el: null,
            });
        });

        markers.forEach((m)=>{

            if (m.axis_el)
                return; // done already in cluster

            let cluster = [ m ];

            markers.forEach((mm)=>{
                if (mm == m)
                    return;
                if (mm.pos.distanceTo(m.pos) < 0.01) {
                    cluster.push(mm);
                }
            });

            for (let i = 0; i < cluster.length; i++) {
                let mm = cluster[i];
                let v_center_offset = (((cluster.length-1)/2)-i) * 1.1; // 1 is height of one label, adding small margin
                const [ axis_el, label_el] = that.makeMark(frames[mm.key], mm.key,
                    layer_axes, layer_labels,
                    .02, h_center, v_center_offset
                );
                mm.axis_el = axis_el;
                mm.label_el = label_el;
            } 
        });

        return markers;
    }

    makeMark(target, label_text, layer_axes, layer_labels, axis_size=.02, h_center=true, v_center=0) {

        let is_selected = this.vars.follow_target && target == this.camera_target;

        if (!is_selected && (layer_axes == DescriptionTFWidget.L_JOINTS || layer_axes == DescriptionTFWidget.L_LINKS)) {
             axis_size = .0015;
        }

        if (is_selected && this.robot_model && label_text == this.robot_model.name) {
            axis_size = 1.0; // big visual for robots coords frame, when base link is selected
        }
  
        const axesHelper = new THREE.AxesHelper(axis_size);       
        axesHelper.material.transparent = true;
        axesHelper.material.opacity = 0.99;
        axesHelper.material.width = 1.0;
        axesHelper.material.depthTest = false;
        axesHelper.material.depthWrite = false;

        target.add(axesHelper);
        axesHelper.layers.set(layer_axes);  

        let label_el = null;
        if (label_text) {
            const el = document.createElement('div');
            el.className = 'marker_label';
            el.title = 'Focus camera here';
            if (is_selected)
                el.className += ' focused';
            if (!h_center)
                el.className += (layer_labels == DescriptionTFWidget.L_JOINT_LABELS ? ' joint' : ' link');
            el.textContent = label_text;
            
            label_el = new CSS2DObject(el);
            let that = this;
            el.addEventListener('pointerdown', function(ev) {
                that.setCameraTarget(target, label_text, true); // label=key, turns on following
                ev.preventDefault();
            });
            target.add(label_el);
            label_el.center.set(h_center ? 0.5 : (layer_labels == DescriptionTFWidget.L_JOINT_LABELS ? 1.0 : 0.0),  // joints left, links right
                                v_center);
            label_el.position.set(0, 0, 0);
            label_el.layers.set(layer_labels);

            // console.log('Making label "'+label_text+'", type='+layer_labels);
        }

        return [ axesHelper, label_el];
    }

    onTFData (topic, tf) {
        if (this.panel.paused || !this.robot_model) //wait for the model
            return;

        for (let i = 0; i < tf.transforms.length; i++) {

            let ns_stamp = tf.transforms[i].header.stamp.sec * 1000000000 + tf.transforms[i].header.stamp.nanosec;
            let id_child = tf.transforms[i].child_frame_id;
            let t = tf.transforms[i].transform;

            if (this.smooth_transforms_queue[id_child] !== undefined && this.smooth_transforms_queue[id_child].stamp > ns_stamp)
                continue; //throw out older transforms

            t.rotation = new Quaternion(t.rotation.x, t.rotation.y, t.rotation.z, t.rotation.w).normalize();
            t.translation = new THREE.Vector3(t.translation.x, t.translation.y, t.translation.z);

            this.smooth_transforms_queue[id_child] = {
                parent: tf.transforms[i].header.frame_id,
                transform: t,
                stamp: ns_stamp
            }
            
            if (id_child == this.robot_model.name) { //this is the base link

                if (!this.ros_space_offset_set) {
                    // moves ros space so that the initial ronot's position and rotation is aligned with the scene's origin
                    // all furher transforms then take place in the ros space

                    this.ros_space.quaternion.copy(this.ros_space_default_rotation.clone().multiply(t.rotation.clone().invert())); // robot aligned with scene
                    let t_pos = t.translation.clone().applyQuaternion(this.ros_space.quaternion);
                    this.ros_space.position.copy(t_pos.clone().negate());

                    this.ros_space_offset_set = true;
                }

                // trim pg
                while (this.pose_graph.length > this.pose_graph_size) { 
                    let rem = this.pose_graph.shift();
                    if (rem.visual) {
                        rem.visual.removeFromParent();
                    }
                }
                
                let pg_node = {
                    pos: t.translation,
                    rot: t.rotation,
                    mat: new THREE.Matrix4(),
                    ns_stamp: ns_stamp,
                };
                pg_node.mat.compose(pg_node.pos, pg_node.rot, new THREE.Vector3(1,1,1))

                if (this.vars.render_pose_graph) {
                    pg_node.visual = new THREE.AxesHelper(.05);
                    pg_node.visual.material.depthTest = false;
                    pg_node.visual.position.copy(pg_node.pos);
                    pg_node.visual.quaternion.copy(pg_node.rot);
                    pg_node.visual.layers.set(DescriptionTFWidget.L_POSE_GRAPH);
                    this.ros_space.add(pg_node.visual); //+z up
                }
                
                this.pose_graph.push(pg_node);
                
                let e = new CustomEvent('pg_updated', { detail: { topic: topic, pg_node: pg_node } } );
                this.dispatchEvent(e);
            }

        }
    }

    controlsChanged() {
        this.controls_dirty = true;
        this.camera_pos.copy(this.camera.position);
    }

    renderDirty() {
        if (!this.renderer)
            return;

        this.render_dirty = true;
    }

    renderingLoop() {

        if (!this.rendering)
            return;

        const lerp_amount = 0.6;
        const cam_lerp_amount = 0.5;
        let that = this;

        if (this.robot_model && this.robot_model.frames) {

            let transform_ch_frames = Object.keys(this.smooth_transforms_queue);
            for (let i = 0; i < transform_ch_frames.length; i++) {

                let id_child = transform_ch_frames[i];
                let id_parent = this.smooth_transforms_queue[id_child].parent;
                let t = this.smooth_transforms_queue[id_child].transform;

                let t_parent = this.robot_model.frames[id_parent];
                let t_child = this.robot_model.frames[id_child];
                if (!t_child)
                    continue; // child node not present in urdf

                if (id_child == this.robot_model.name) { // robot base frame => move this.robot in ros_space

                    let new_robot_world_position = new THREE.Vector3();

                    if (!this.vars.fix_robot_base) {
                        let pos = t.translation;

                        let old_robot_world_position = new THREE.Vector3();
                        this.robot.getWorldPosition(old_robot_world_position);

                        if (this.robot_pose_initialized)
                            this.robot.position.lerp(pos, lerp_amount);
                        else
                            this.robot.position.copy(pos);

                        this.robot.getWorldPosition(new_robot_world_position);

                        let d_pos = new THREE.Vector3().subVectors(new_robot_world_position, old_robot_world_position);

                        if (this.robot_pose_initialized)
                            this.camera_pos.add(d_pos); // move camera by d
                        this.light.position.add(d_pos);

                    } else {
                        this.robot.getWorldPosition(new_robot_world_position); // only get world pos (to rotate around)
                    }
                    
                    let old_robot_world_rotation = new THREE.Quaternion();
                    this.robot.getWorldQuaternion(old_robot_world_rotation);

                    // let rot = new THREE.Quaternion(t.rotation.x, t.rotation.y, t.rotation.z, t.rotation.w);
                    // rot = rot.multiply(this.rot_offset);

                    if (this.robot_pose_initialized)
                        this.robot.quaternion.slerp(t.rotation, lerp_amount); //set rot always
                    else
                        this.robot.quaternion.copy(t.rotation);

                    let new_robot_world_rotation = new THREE.Quaternion();
                    this.robot.getWorldQuaternion(new_robot_world_rotation);

                    // let d_rot_rad = old_robot_world_rotation.angleTo(new_robot_world_rotation);
                    let d_rot = new_robot_world_rotation.multiply(old_robot_world_rotation.invert());
                    // console.log(d_rot_rad);

                    if (this.robot_pose_initialized) {
                        this.camera_pos.sub(new_robot_world_position)
                                    //    .applyAxisAngle(new THREE.Vector3(0,1,0), d_rot_rad)
                                   .applyQuaternion(d_rot)
                                   .add(new_robot_world_position);
                    }

                    if (!this.vars.follow_target && this.last_camera_url_update+1000 < Date.now()) {
                        setTimeout(()=>{
                            // saves new camera pos in relation to robot as it moves around
                            this.last_camera_url_update = Date.now();
                            that.panel.ui.updateUrlHash(); 
                        }, 0);
                    }
                    
                    this.robot_pose_initialized = true;
                    this.light.target = this.robot;                    

                } else if (t_child && t_parent) {
                    
                    let orig_p = t_child.parent;

                    t_parent.attach(t_child);

                    if (this.robot_pose_initialized) {
                        t_child.position.lerp(new THREE.Vector3(t.translation.x, t.translation.y, t.translation.z), lerp_amount);
                        t_child.quaternion.slerp(new THREE.Quaternion(t.rotation.x, t.rotation.y, t.rotation.z, t.rotation.w), lerp_amount);
                    } else {
                        t_child.position.copy(new THREE.Vector3(t.translation.x, t.translation.y, t.translation.z));
                        t_child.quaternion.copy(new THREE.Quaternion(t.rotation.x, t.rotation.y, t.rotation.z, t.rotation.w));
                    }
                    
                    orig_p.attach(t_child);
    
                } else {
                    console.warn('tf transform not found: '+id_parent+' > '+id_child);
                }

                this.renderDirty();
            }

        }

        if (this.robot_model && this.vars.follow_target && this.camera_pose_initialized) {

            let pos_to_maintain = new THREE.Vector3().copy(this.camera_pos);
            
            this.camera.position.lerp(this.camera_pos, cam_lerp_amount);
            
            if (this.camera_target) {
                let new_target_pos = new THREE.Vector3();
                this.camera_target.getWorldPosition(new_target_pos);
                if (this.camera_target_pose_initialized) {
                    this.camera_target_pos.position.copy(this.camera_target_pos.position.lerp(new_target_pos, cam_lerp_amount));
                } else {
                     this.camera_target_pos.position.copy(new_target_pos);
                     this.camera_target_pose_initialized = true;
                }
                // this.camera.lookAt(this.camera_target_pos);
                // console.log('Focusing on ['+new_target_pos.x+';'+new_target_pos.y+';'+new_target_pos.z+']');
            }

            this.controls.update();
            this.camera_pos = pos_to_maintain;
        }

        if (this.robot_model) {
            this.camera_pose_initialized = true;
        }

        if ((this.controls_dirty || this.render_dirty) && this.robot_model) {
            this.controls_dirty = false;
            this.render_dirty = false;
            try {
                this.renderer.render(this.scene, this.camera);
                this.labelRenderer.render(this.scene, this.camera);
                this.rendering_error_logged = false;
            } catch (e) {

                if (!this.rendering_error_logged) {
                    this.rendering_error_logged = true;
                    console.error('Error caught while rendering', e);

                    this.scene.traverse(function(obj) {
                        var s = '';
                        var obj2 = obj;
                        while(obj2 !== that.scene) {
                            s += '-';
                            obj2 = obj2.parent;
                        }
                        console.log(s + obj.type + ' ' + obj.name+ ' mat: '+obj.material);
                    });
                } 
                
            }
        }        

        requestAnimationFrame((t) => this.renderingLoop());
    }

    // get_latest_pg_ns_stamp() {
    //     if (!this.pose_graph || !this.pose_graph.length)
    //         return NaN;

    //     return this.pose_graph[this.pose_graph.length-1].ns_stamp;
    // }

}