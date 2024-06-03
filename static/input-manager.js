import { cos } from 'three/examples/jsm/nodes/Nodes.js';
import { isIOS, lerp, isTouchDevice } from './lib.js';
import { Handle_Shortcut } from '/static/input-drivers.js';
import * as THREE from 'three';

export class InputManager {

    constructor(client) {

        this.client = client;
        this.ui = null; // ui constructor assigms ref

        this.registered_drivers = {}; // id => class; all available
        this.enabled_drivers = {}; // gp_type => driver[]; enabled by robot

        this.controllers = {}; //id => gp ( touch)
        this.edited_controller = null;

        this.robot_defaults = null; // defaults from robot

        this.profiles = null;
        this.current_profile = null;

        // this.default_shortcuts_config = {};
        // this.shortcuts_config = null;
        // this.last_buttons = [];
        // this.editor_listening = false;

        // this.last_gp_loaded = null;
        this.loop_delay = 33.3; // ms, 30Hz updates
        
        // this.enabled = false; 
        this.loop_running = false;
        this.controller_enabled_cb = $('#controller-enabled-cb');
        this.controller_enabled_cb.prop('checked', false);
        this.input_status_icon = $('#input-status-icon');
        this.debug_output_panel = $('#gamepad-output-panel');

        let that = this;
        this.zero = new THREE.Vector2(0,0);

        // this.show_icon = this.load_some_gamepad_once_connected(); // once you'be connected a gamepad, the icon will stay on
        
        // this.default_configs = {};
        // this.profiles = {}; // gp type => id => profile[]

        // this.touch_gamepad = false;
        this.last_touch_input = {};
       
        client.on('input_config', (drivers, defaults)=>{ that.set_config(drivers, defaults); });
        // client.on('touch_config', (drivers, defaults)=>{ that.set_config('touch', drivers, defaults); });
        // client.on('kb_config', (drivers, defaults)=>{ that.set_config('kb', drivers, defaults); });

        this.input_status_icon.click(() => {
            // if (!that.initiated)
            //     return; //wait 
            $('#keyboard').removeClass('open');
            if (!$('#gamepad').hasClass('open')) {
                $('#gamepad').addClass('open');
                $('BODY').addClass('gamepad-editing');
                if (this.edited_controller && this.edited_controller.type == 'touch') {
                    $('#touch-gamepad-left').appendTo($('#gamepad-touch-left-zone'));
                    $('#touch-gamepad-right').appendTo($('#gamepad-touch-right-zone'));
                    $('#touch-gamepad-left > .Gamepad-anchor').css('inset', '60% 50% 40% 50%');
                    $('#touch-gamepad-right > .Gamepad-anchor').css('inset', '60% 50% 40% 50%');
                }
                $('#input-underlay').css('display', 'block')
                    .unbind()
                    .click((ev)=>{
                        $(ev.target).unbind();
                        that.input_status_icon.click();
                    })
            } else {
                $('#gamepad').removeClass('open');
                $('BODY').removeClass('gamepad-editing');
                if (this.edited_controller && this.edited_controller.type == 'touch') {
                    $('#touch-gamepad-left').appendTo($('BODY'));
                    $('#touch-gamepad-right').appendTo($('BODY'));
                    $('#touch-gamepad-left > .Gamepad-anchor').css('inset', '60% 50% 40% 50%');
                    $('#touch-gamepad-right > .Gamepad-anchor').css('inset', '60% 50% 40% 50%');
                }
                $('#input-underlay').css('display', '').unbind();
            }
        });

        window.addEventListener('gamepadconnected', (ev) => this.on_gamepad_connected(ev));
        window.addEventListener('gamepaddisconnected', (ev) => this.on_gamepad_disconnected(ev));
    
        $('#gamepad_settings .tab').click((ev)=>{
            if ($(ev.target).hasClass('active'))
                return;
            $('#gamepad_settings .tab')
                .removeClass('active');
            
            $('#gamepad_settings .panel')
                .removeClass('active');
            let open = '';
            let open_tab = ev.target;
            switch (ev.target.id) {
                case 'gamepad-axes-tab': open = '#gamepad-axes-panel'; break;
                case 'gamepad-buttons-tab': open = '#gamepad-buttons-panel'; break;
                case 'gamepad-output-tab': open = '#gamepad-output-panel'; break;
                case 'gamepad-settings-tab':
                case 'gamepad-profile-unsaved-warn':
                    open = '#gamepad-settings-panel';
                    open_tab = '#gamepad-settings-tab';
                    break;
                default: return;
            }
            $(open_tab) 
                .addClass('active')
            $(open)
                .addClass('active');
        });
        $('#gamepad-profile-unsaved-warn').click((ev)=>{
            $('#gamepad-settings-tab').click();
        });

        this.controller_enabled_cb.change((ev) => {
            that.edited_controller.enabled = $(ev.target).prop('checked');
            that.save_user_controller_enabled(that.edited_controller);
            that.make_controller_icons();
            // if (that.edited_controller.enabled) {
            //     // $('#gamepad').addClass('enabled');
            //     // that.disable_kb_on_conflict();
            // } else {
            //     // $('#gamepad').removeClass('enabled');
            // }
        });

        let close_profile_menu = () => {
            $('#profile-buttons').removeClass('open');
            $('#input-manager-overlay')
                .css('display', 'none')
                .unbind();
        }

        this.editing_profile_basics = false;
        $('#profile-buttons > .icon, #profile-unsaved-warn').click((ev)=>{
            if (that.editing_profile_basics) {
                that.close_profile_edit();
                return;
            }

            if ($('#profile-buttons').hasClass('open')) {
                close_profile_menu();
            } else {
                $('#profile-buttons').addClass('open');
                $('#input-manager-overlay')
                    .css('display', 'block')
                    .unbind()
                    .click((ev_u)=>{
                        close_profile_menu();
                    });
            }
        });

        $('#save-input-profile').click((ev)=>{
            close_profile_menu();
            that.save_user_gamepad_profile_config(that.edited_controller, that.current_profile);
        });

       
        $('#edit-input-profile').click(()=>{
            close_profile_menu();
            $('#gamepad-settings-container').addClass('editing_profile_basics');
            that.editing_profile_basics = true;
            $('#input-profile-edit-label')
                .val(that.profiles[that.current_profile].label)
                .unbind()
                .change((ev)=>{
                    that.profiles[that.current_profile].label = $('#input-profile-edit-label').val();
                    that.check_profile_basics_saved(that.current_profile, false);
                    that.make_profile_selector_ui();
                });
            $('#input-profile-edit-id')
                .val(that.current_profile)
                .unbind()
                .change((ev)=>{
                    let new_id = $('#input-profile-edit-id').val();
                    let old_id = that.current_profile;
                    that.profiles[new_id] = that.profiles[old_id];
                    that.profiles[new_id].id = new_id;
                    delete that.profiles[old_id];
                    that.current_profile = new_id;
                    Object.values(that.controllers).forEach((c)=>{
                        c.profiles[new_id] = c.profiles[old_id];
                        delete c.profiles[old_id];
                    });
                    that.check_profile_basics_saved(that.current_profile, false);
                    that.make_ui();
                });
        });

        $('#delete-input-profile')
        .click((ev)=>{
            if ($(ev.target).hasClass('warn')) {
                that.delete_current_profile();
                $(ev.target).removeClass('warn');
                close_profile_menu();
                return;
            } else {
                $(ev.target).addClass('warn');
            }
        })
        .blur((ev)=>{
            $(ev.target).removeClass('warn');
         });

        $('#input-profile-json').click((ev)=>{
            that.copy_gamepad_profile_json(that.edited_controller, that.current_gamepad.current_profile);
        });

        if (isTouchDevice())
            this.make_touch_gamepad();
        
        this.make_keyboard();
        this.last_keyboard_input = {};
        document.addEventListener('keydown', (ev) => that.on_keyboard_key_down(ev, that.controllers['keyboard']));
        document.addEventListener('keyup', (ev) => that.on_keyboard_key_up(ev, that.controllers['keyboard']));
        window.addEventListener("blur", (event) => {
            // console.warn('Window lost focus');
            that.last_keyboard_input = {};
            // that.update_input_ui();
        });

        this.make_ui();
    }

    close_profile_edit() {
        $('#gamepad-settings-container').removeClass('editing_profile_basics');
        this.editing_profile_basics = false;
        $('#input-profile-edit-label').unbind();
        $('#input-profile-edit-id').unbind();
    }

    set_config(enabled_drivers, robot_defaults) {
        console.info(`Input manager got robot config; enabled_drivers=[${enabled_drivers.join(', ')}]:`, robot_defaults);

        this.enabled_drivers = enabled_drivers;

        this.robot_defaults = robot_defaults;
        
        let user_defaults = localStorage.getItem('user-input-config:'+this.client.id_robot);
        this.user_defaults = user_defaults ? JSON.parse(user_defaults) : {};

        console.log('Loaded user input defaults: ', this.user_defaults);

        if (!this.profiles) {
            this.profiles = {};

            // robot defined profiles
            Object.keys(robot_defaults).forEach((id_profile)=>{
                if (this.current_profile === null)
                    this.current_profile = id_profile; // 1st is default
                if (!this.profiles[id_profile]) {
                    let label = robot_defaults[id_profile].label ? robot_defaults[id_profile].label : id_profile;
                    this.profiles[id_profile] = {
                        label: label,
                        id: id_profile,
                        id_saved: id_profile,
                        label_saved: label,
                        saved: true,
                        basics_saved: true,
                    };
                }
            });

            // override with local cookies
            Object.keys(this.user_defaults).forEach((id_profile)=>{
                if (!this.profiles[id_profile]) {
                    let label = this.user_defaults[id_profile].label ? this.user_defaults[id_profile].label : id_profile;
                    this.profiles[id_profile] = {
                        label: label,
                        saved: true,
                    };
                } else {
                    if (this.user_defaults[id_profile].label) 
                        this.profiles[id_profile].label = this.user_defaults[id_profile].label;
                }
            });

            let last_user_profile = this.load_last_user_profile();
            console.log('Loaded last input profile :', last_user_profile);

            if (last_user_profile && this.profiles[last_user_profile]) {
                this.current_profile = last_user_profile;
            }

        }
        
        this.make_profile_selector_ui();

        Object.values(this.controllers).forEach((c)=>{
            this.init_controller(c);
        });
    }

    init_controller(c) {

        if (this.robot_defaults === null) // wait for robot config & cookie overrides
            return;

        if (!c.saved_profiles) 
            c.saved_profiles = {};

        if (!c.profiles) { // only once

            // let robot_defaults = this.robot_defaults[c.type];

            c.profiles = {};
            // c.current_profile = null;
            c.enabled = c.type == 'touch' ? true : this.load_user_controller_enabled(c.id);

            // let all_profile_ids = [].concat(this.saved_profile_ids); // shallow copy

            // let robot_defaults_by_id = {};
            // if (robot_defaults.profiles) {
            //     robot_defaults.profiles.forEach((profile_default_cfg)=>{
            //         let id_profile = profile_default_cfg.id;
            //         if (!id_profile) {
            //             console.error('Controller profile config for '+c.type+' missing id', profile_default_cfg)
            //             return;
            //         }
            //         robot_defaults_by_id[id_profile] = profile_default_cfg;
            //         if (all_profile_ids.indexOf(id_profile) < 0) {
            //             all_profile_ids.push(id_profile);
            //         }
            //     });
            // }

            // let saved_user_gamepad_config = this.load_user_gamepad_config(gamepad.id);
            // console.log('Loaded user config for gamepad "'+gamepad.id+'":', saved_user_gamepad_config);

            Object.keys(this.profiles).forEach((id_profile)=>{

                // let id_profile = profile_default_cfg.id;
                let profile_default_cfg = {};
                if (this.robot_defaults[id_profile] && this.robot_defaults[id_profile][c.type])
                    profile_default_cfg = this.robot_defaults[id_profile][c.type];
                let user_defaults = this.load_user_controller_profile_config(c, id_profile);
                if (user_defaults) {
                    console.log(c.id+' loaded user defults for '+id_profile, user_defaults);
                    profile_default_cfg = user_defaults;
                }

                if (!profile_default_cfg.driver) {
                    profile_default_cfg.driver = this.enabled_drivers[0];
                    console.warn('Controller profile '+id_profile+' for '+c.type+' missing driver, fallback='+profile_default_cfg.driver+'; config=', profile_default_cfg)
                }

                let profile = {
                    driver: profile_default_cfg.driver,
                    default_driver_config: {},
                    default_axes_config: profile_default_cfg.axes
                }
                
                if (profile_default_cfg.driver_config) {
                    profile.default_driver_config[profile_default_cfg.driver] = profile_default_cfg.driver_config;
                }

                c.profiles[id_profile] = profile;

                this.init_profile(c, profile);
                this.set_saved_controller_profile_state(c, id_profile);
                c.profiles[id_profile].saved = true;

                // if (profile_default_cfg.default) { // default profile by robot
                //     c.current_profile = id_profile;
                // }
            });
        

            // if (last_user_gamepad_profile && gamepad.profiles[last_user_gamepad_profile]) {
            //     gamepad.current_profile = last_user_gamepad_profile;  // overrride default profile by user
            // }

            console.log('Initiated profiles for gamepad '+c.id);
        }

        this.make_controller_icons();        

        if (this.edited_controller == c) {
            this.make_ui();
        }

        if (!this.loop_running) {
            this.loop_running = true;
            this.run_loop();
        }
    }

    get_profile_config(c, id_profile, only_assigned=false) {
        let profile = c.profiles[id_profile];
        let driver = profile.driver_instances[profile.driver];

        let data = {
            // id: profile.id,
            // label: profile.label,
            driver: profile.driver,
            driver_config: driver.get_config(),
            axes: [],
            buttons: [],
        };
        
        for (let i = 0; i < driver.axes.length; i++) {
            if (only_assigned && !driver.axes[i].driver_axis) {
                continue;
            }
            let axis_data = {
                axis: i,
                driver_axis: driver.axes[i].driver_axis,
                dead_min: driver.axes[i].dead_min,
                dead_max: driver.axes[i].dead_max,
                offset: driver.axes[i].offset,
                scale: driver.axes[i].scale,
            }
            if (driver.axes[i].mod_func) {
                axis_data['mod_func'] = {
                    type: driver.axes[i].mod_func,
                    velocity_src: driver.axes[i].scale_by_velocity_src,
                    slow_multiplier: driver.axes[i].scale_by_velocity_mult_min,
                    fast_multiplier: driver.axes[i].scale_by_velocity_mult_max,
                }
            }
            data.axes.push(axis_data);
        }

        return data;
    }

    set_saved_controller_profile_state(c, id_profile) {
        let profile = c.profiles[id_profile];
        let driver = profile.driver_instances[profile.driver];
        driver.set_saved_state();
        
        let saved_data = this.get_profile_config(c, id_profile, false);
        profile.saved_state = saved_data;
    }

    check_profile_basics_saved(id_profile, update_ui = true) {
        
        let profile = this.profiles[id_profile];
        function compare() {
            if (profile.id != profile.id_saved)
                return false;
            if (profile.label != profile.label_saved)
                return false;
            return true;
        }
        profile.basics_saved = compare();

        if (update_ui)
            this.make_profile_selector_ui();
    }
    
    check_controller_profile_saved(c, id_profile, update_ui = true) {

        let live_profile = c.profiles[id_profile];
        let saved_profile = live_profile.saved_state;

        function compare(profile, saved) {
            if (!profile || !saved)
                return false;
            // if (profile.id != saved.id)
            //     return false;
            // if (profile.label != saved.label)
            //     return false;
            if (profile.driver != saved.driver)
                return false;
            let driver = profile.driver_instances[profile.driver];
            if (!driver.check_saved())
                return false;
            
            if (driver.axes.length != saved.axes.length)
                return false;

            for (let i = 0; i < driver.axes.length; i++) {
                if (driver.axes[i].driver_axis != saved.axes[i].driver_axis)
                    return false;
                if (driver.axes[i].dead_min != saved.axes[i].dead_min)
                    return false;
                if (driver.axes[i].dead_max != saved.axes[i].dead_max)
                    return false;
                if (driver.axes[i].offset != saved.axes[i].offset)
                    return false;
                if (driver.axes[i].scale != saved.axes[i].scale)
                    return false;

                let has_mod_func = driver.axes[i].mod_func !== null && driver.axes[i].mod_func !== undefined && driver.axes[i].mod_func !== false && driver.axes[i].mod_func !== "";
                let has_saved_mod_func = saved.axes[i].mod_func !== null && saved.axes[i].mod_func !== undefined; //obj

                if (has_mod_func != has_saved_mod_func) { 
                    // console.warn('live v saved mod_func: ', driver.axes[i].mod_func, saved.axes[i].mod_func)
                    return false;
                } else if (driver.axes[i].mod_func && saved.axes[i].mod_func) {
                    if (driver.axes[i].mod_func != saved.axes[i].mod_func.type) 
                        return false;
                    if (driver.axes[i].scale_by_velocity_src != saved.axes[i].mod_func.velocity_src) 
                        return false;
                    if (driver.axes[i].scale_by_velocity_mult_min != saved.axes[i].mod_func.slow_multiplier) 
                        return false;
                    if (driver.axes[i].scale_by_velocity_mult_max != saved.axes[i].mod_func.fast_multiplier) 
                        return false;
                }
                
            }

            return true; // all checks up 
        }

        let match = compare(live_profile, saved_profile);

        // console.info(`Profile ${id_profile} saved: `, match, live_profile, saved_profile);

        if (!match && live_profile.saved) {
            live_profile.saved = false;
            this.profiles[id_profile].saved = false;
            console.log('Profile '+id_profile+' not saved');
            if (update_ui)
                this.make_profile_selector_ui();
           
        } else if (match && !live_profile.saved) {
            live_profile.saved = true;
            if (!this.profiles[id_profile].saved) {
                let all_saved = true;
                Object.values(this.controllers).forEach((cc)=>{
                    if (cc == c)
                        return;
                    if (!cc.profiles[id_profile].saved)
                        all_saved = false;
                });
                this.profiles[id_profile].saved = all_saved;
            }
            console.log('Profile '+id_profile+' saved: '+this.profiles[id_profile].saved);
            if (update_ui)
                this.make_profile_selector_ui();

          
        }
    }

    make_button(driver) {
        if (driver.i_btn === undefined)
            driver.i_btn = -1;
        driver.i_btn++;
        let new_btn = { 
            i: driver.i_btn,
            id_src: null, // id on gamepad
            src_label: null, // hr key name
            pressed: false,
            touched: false,
            raw: null,
            driver_btn: null, // output as diver button
            driver_axis: null, // output as driver axis
            action: null, // performs all other actions
        }
        driver.buttons.push(new_btn);
        return new_btn;
    }

    init_profile(c, profile) {

        if (!profile.driver_instances) {
            profile.driver_instances = {};
        }

        if (!profile.driver_instances[profile.driver]) {

            //init driver
            profile.driver_instances[profile.driver] = new this.registered_drivers[profile.driver](this);
            if (profile.default_driver_config && profile.default_driver_config[profile.driver]) {
                profile.driver_instances[profile.driver].set_config(profile.default_driver_config[profile.driver]);
            } else {
                profile.driver_instances[profile.driver].set_config({}); // init writer
            }

            let driver = profile.driver_instances[profile.driver];
            let driver_axes_ids = Object.keys(driver.get_axes());

            profile.buttons = []; 
            driver.buttons = []; // driver independent buttons
            driver.axes = [];

            function make_axis(i_axis, default_dead_zone) {
                let axis_cfg = null;
                if (profile.default_axes_config) {
                    profile.default_axes_config.forEach((cfg)=>{
                        if (cfg.axis === i_axis && driver_axes_ids.indexOf(cfg.driver_axis) > -1) {
                            axis_cfg = cfg;
                            return;
                        }
                    });
                }

                let new_axis = { 
                    i: i_axis,
                    raw: null,
                    driver_axis: axis_cfg && axis_cfg.driver_axis ? axis_cfg.driver_axis : null,
                    dead_min: axis_cfg && axis_cfg.dead_min !== undefined ? axis_cfg.dead_min : -default_dead_zone,
                    dead_max: axis_cfg && axis_cfg.dead_max !== undefined ? axis_cfg.dead_max : default_dead_zone,
                    offset: axis_cfg && axis_cfg.offset !== undefined ? axis_cfg.offset : 0.0,
                    scale: axis_cfg && axis_cfg.scale !== undefined ? axis_cfg.scale : 1.0,                
                }

                if (axis_cfg && axis_cfg.mod_func && axis_cfg.mod_func.type) {
                    switch (axis_cfg.mod_func.type) {
                        case 'scale_by_velocity':
                            new_axis.mod_func = axis_cfg.mod_func.type;
                            new_axis.scale_by_velocity_src = axis_cfg.mod_func.velocity_src;
                            new_axis.scale_by_velocity_mult_min = axis_cfg.mod_func.slow_multiplier !== undefined ? axis_cfg.mod_func.slow_multiplier : 1.0;
                            new_axis.scale_by_velocity_mult_max = axis_cfg.mod_func.fast_multiplier !== undefined ? axis_cfg.mod_func.fast_multiplier : 1.0;
                            break;
                        default:
                            break
                    }
                }

                return new_axis;
            }

            if (c.type == 'touch') {
                for (let i_axis = 0; i_axis < 4; i_axis++) {
                    let new_axis = make_axis(i_axis, 0.01);
                    if (new_axis) {
                        driver.axes.push(new_axis);
                    }
                }
                for (let i_btn = 0; i_btn < 3; i_btn++) { //start with 3, users can add mode but space will be sometimes limitted
                    let new_button = this.make_button(driver);
                }
            } else if (c.type == 'keyboard') {
                for (let i_btn = 0; i_btn < 5; i_btn++) { //start with 5, users can add more
                    let new_button = this.make_button(driver);
                }
            } else if (c.type == 'gamepad') {
                const gp = navigator.getGamepads()[c.gamepad.index];
                console.log('Making axes & buttons for gamepad', c.gamepad);
                for (let i_axis = 0; i_axis < gp.axes.length; i_axis++) {
                    let new_axis = make_axis(i_axis, 0.1); //default deadzone bigger than touch
                    if (new_axis) {
                        new_axis.needs_reset = true; //waits for 1st non-zero signals
                        driver.axes.push(new_axis);
                    }
                }
                for (let i_btn = 0; i_btn < 5; i_btn++) { //start with 5, users can add more
                    let new_button = this.make_button(driver);
                }
            }
        }

    }
    
    delete_current_profile() {
    
        let id_delete = this.current_gamepad.current_profile;
        let saved_id_delete = this.current_gamepad.profiles[id_delete].saved_state.id;
        let old_profile_ids = Object.keys(this.current_gamepad.profiles);
        let pos = old_profile_ids.indexOf(id_delete);
        
        console.log('Deleting profile '+id_delete+' (saved id was '+saved_id_delete+')');

        // remove profile conf from cookie
        let cookie_conf = 'gamepad-profile-config:'+this.client.id_robot+':'+this.current_gamepad.id+':'+saved_id_delete;
        localStorage.removeItem(cookie_conf);

        let cookie_conf_list = 'user-gamepad-profiles:'+this.client.id_robot+':'+this.current_gamepad.id;
        let custom_profile_ids = localStorage.getItem(cookie_conf_list);
        if (custom_profile_ids) {
            custom_profile_ids = JSON.parse(custom_profile_ids)
            let pos = custom_profile_ids.indexOf(saved_id_delete);
            if (pos > -1) {
                custom_profile_ids.splice(pos, 1);
            }
            console.log('saving updated custom_profile_ids', custom_profile_ids); 
            localStorage.setItem(cookie_conf_list, JSON.stringify(custom_profile_ids));
        }

        delete this.current_gamepad.profiles[id_delete];
        let remaining_profile_ids = Object.keys(this.current_gamepad.profiles);

        if (remaining_profile_ids.length == 0) {
            console.log('No profile to autoselect, making new');
            
            let id_new_profile = 'Profile-'+Date.now();
            this.current_gamepad.profiles[id_new_profile] = {
                // id: id_new_profile,
                driver: this.enabled_drivers[this.current_gamepad.type][0], // copy current as default
                // label: id_new_profile,
            }
            this.init_profile(this.current_gamepad.profiles[id_new_profile]);
            this.current_gamepad.current_profile = id_new_profile;

        } else {
            while (!remaining_profile_ids[pos] && pos > 0) {
                pos--;
            }
            let id_select = remaining_profile_ids[pos];
            console.log('Autoselecting '+id_select);
            this.current_gamepad.current_profile = id_select;
        }

        this.make_ui();
    }

    save_last_user_profile(id_profile) {
        localStorage.setItem('last-input-profile:' + this.client.id_robot, id_profile);
    }

    load_last_user_profile() {
        return localStorage.getItem('last-input-profile:' + this.client.id_robot);
    }

    copy_gamepad_profile_json(gamepad, id_profile) {
        let profile_data = this.get_profile_config(gamepad, id_profile, true);
        navigator.clipboard.writeText(JSON.stringify(profile_data, null, 4));
        console.log('Copied json:', profile_data);
        $('#gamepad-profile-json-bubble').css('display', 'block');
        window.setTimeout(()=>{
            $('#gamepad-profile-json-bubble').css('display', 'none');
        }, 3000);
    }

    save_edited_profile_ids() {
        
        //TODO
        let cookie_conf_list = 'user-input-profiles:'+this.client.id_robot;
        let custom_profile_ids = localStorage.getItem(cookie_conf_list);
        // if (!custom_profile_ids) {
        //     custom_profile_ids = [];
        // } else {
        //     custom_profile_ids = JSON.parse(custom_profile_ids)
        // }
    }

    save_user_controller_profile_config(c, id_profile) {
        let profile = c.profiles[id_profile];
        let profile_data = this.get_profile_config(c, id_profile, true); //filters assigned axes

        console.log('Saving profile '+id_profile+' for '+c.id, profile_data);
        let cookie_conf = 'controller-profile-config:'+this.client.id_robot+':'+c.id+':'+id_profile;
        localStorage.setItem(cookie_conf, JSON.stringify(profile_data));
       
        // console.log('loaded custom_profile_ids', custom_profile_ids);

        if (custom_profile_ids.indexOf(id_profile) === -1)
            custom_profile_ids.push(id_profile);
        if (profile.saved_state.id != id_profile) { // id changed, remove old
            let pos = custom_profile_ids.indexOf(profile.saved_state.id);
            if (pos > -1) {
                custom_profile_ids.splice(pos, 1);
            }
            let deleted_cookie_conf = 'controller-profile-config:'+this.client.id_robot+':'+c.id+':'+profile.saved_state.id;
            localStorage.removeItem(deleted_cookie_conf);
        }

        console.log('saving custom_profile_ids', custom_profile_ids); 
        localStorage.setItem(cookie_conf_list, JSON.stringify(custom_profile_ids));

        this.set_saved_controller_profile_state(c, id_profile);
        this.check_controller_profile_saved(c, id_profile);
    }

    load_user_controller_profile_config(c, id_profile) {
        let cookie_conf = 'controller-profile-config:'+this.client.id_robot+':'+c.id+':'+id_profile;
        let val = localStorage.getItem(cookie_conf);

        if (val)
            return JSON.parse(val);
        else
            return null;
    }

    disable_kb_on_conflict() {

        if (!this.current_gamepad.enabled)
            return;

        let kb = this.client.ui.keyboard;
        if (kb && kb.enabled && kb.current_driver.id == this.current_gamepad.profiles[this.current_gamepad.current_profile].driver) {
            $('#keyboard_enabled').click(); // avoid same driver coflicts
        }
    }

    register_driver(id_driver, driver_class) {
        if (this.registered_drivers[id_driver])
            return;

        this.registered_drivers[id_driver] = driver_class;
    }

    make_profile_selector_ui() {
        // profile selection
        let profile_opts = [];
        let that = this;

        console.log('Current profile is ', this.current_profile);

        if (!this.profiles)
            return;

        let profile_ids = Object.keys(this.profiles);
        for (let i = 0; i < profile_ids.length; i++) {
            let id_profile = profile_ids[i];
            let label = this.profiles[id_profile].label ? this.profiles[id_profile].label : id_profile;
            if (!this.profiles[id_profile].saved || !this.profiles[id_profile].basics_saved)
                 label = label + ' (edited)';
            profile_opts.push($('<option value="'+id_profile+'"' + (this.current_profile == id_profile ? ' selected' : '') + '>'+label+'</option>'));
        }
        profile_opts.push($('<option value="+">New profile...</option>'));
        $('#input-profile-select')
            .empty().attr({
                'disabled': false,
                'autocomplete': 'off'
            })
            .append(profile_opts);
        
        $('#input-profile-select').unbind().change((ev)=>{
            let val = $(ev.target).val()
            console.log('Selected profile val '+val);
            if (that.editing_profile_basics) {
                that.close_profile_edit();
            }
            // let current_profile = that.current_gamepad.profiles[that.current_gamepad.current_profile];
            if (val != '+') {
                let current_profile = that.profiles[that.current_profile];
                // current_profile.axes_scroll_offset = $('#gamepad-axes-panel').scrollTop();
                // current_profile.buttons_scroll_offset = $('#gamepad-buttons-panel').scrollTop();
                that.current_profile = $(ev.target).val();
                that.make_ui();
            } else {
                let id_new_profile = 'Profile-'+Date.now();
                that.profiles[id_new_profile] = {
                    // id: id_new_profile,
                    // driver: current_profile.driver, // copy current as default
                    // label: id_new_profile,
                }
                that.init_profile(that.edited_controller, that.profiles[id_new_profile]);
                that.current_profile = id_new_profile;
                that.make_ui();
                $('#gamepad_settings #gamepad-settings-tab').click();
                // that.save_user_gamepad_config(); // save the new profile right away
            }
            // that.save_last_user_gamepad_profile(
            //     that.current_gamepad.id,
            //     that.current_gamepad.current_profile
            // );
        });

        if (this.profiles[this.current_profile].saved && this.profiles[this.current_profile].basics_saved) {
            $('#gamepad_settings').removeClass('unsaved');
        } else {
            $('#gamepad_settings').addClass('unsaved');
        }
    }

    make_controller_config_ui() {

        let that = this;

        if (!this.edited_controller || !this.enabled_drivers) {
            $('#gamepad-profile-config').html('<div class="line"><span class="label">Input source:</span><span class="static_val">N/A</span></div>');
            // $('#gamepad-settings-panel').removeClass('has-buttons');
        } else {
            
            let lines = [];

            let label = this.edited_controller.id;
            if (this.edited_controller.type == 'touch')
                label = 'Virtual Gamepad (Touch UI)';
            if (this.edited_controller.type == 'keyboard')
                label = 'Keyboard';

            let line_source = $('<div class="line"><span class="label">Input source:</span><span class="static_val">'
                            + label
                            + '</span></div>');
            lines.push(line_source);

            if (this.current_profile) {
                let profile = this.edited_controller.profiles[this.current_profile];

                //profile id
                // let line_id = $('<div class="line"><span class="label">Profile ID:</span></div>');
                // let inp_id = $('<input type="text" inputmode="url" value="'+profile.id+'"/>');
                // inp_id.change((ev)=>{
                //     let val = $(ev.target).val();
                //     if (this.current_gamepad.current_profile == profile.id) {
                //         this.current_gamepad.current_profile = val;
                //     }
                //     this.current_gamepad.profiles[val] = this.current_gamepad.profiles[profile.id];
                //     delete this.current_gamepad.profiles[profile.id];
                //     profile.id = val;
                //     console.log('Profile id changed to: '+profile.id);
                //     that.save_last_user_gamepad_profile( // id changed
                //         that.current_gamepad.id,
                //         val
                //     );
                //     that.check_profile_saved(that.current_gamepad, profile.id, false);
                //     that.make_profile_selector_ui();
                // });

                // inp_id.appendTo(line_id);
                // lines.push(line_id);

                // //profile name
                // let line_name = $('<div class="line"><span class="label">Profile name:</span></div>');
                // let inp_name = $('<input type="text" value="'+profile.label+'"/>');
                // inp_name.change((ev)=>{
                //     let val = $(ev.target).val();
                //     profile.label = val;
                //     console.log('Profile name changed to: '+profile.label);
                //     that.check_profile_saved(that.current_gamepad, profile.id, false);
                //     that.make_profile_selector_ui();
                // });

                // inp_name.appendTo(line_name);
                // lines.push(line_name);

                //driver
                let line_driver = $('<div class="line"><span class="label">Output driver:</span></div>');
                let driver_opts = [];
                
                if (!this.enabled_drivers || !this.enabled_drivers.length) {
                    console.error('No enabled drivers for '+this.edited_controller.id+' (yet?)');
                    return;
                }
               
                for (let i = 0; i < this.enabled_drivers.length; i++) {
                    let id_driver = this.enabled_drivers[i];
                    driver_opts.push('<option value="'+id_driver+'"'
                                      + (profile.driver == id_driver ? ' selected' : '')
                                      + '>'+id_driver+'</option>')
                }
                let inp_driver = $('<select id="gamepad-profile-driver-select">'
                                 + driver_opts.join('')
                                 + '</select>');
    
                inp_driver.appendTo(line_driver);
                inp_driver.change((ev)=>{
                    let val = $(ev.target).val();
                    console.log('profile driver changed to '+val);
                    profile.driver = val;
                    that.init_profile(that.edited_controller, profile);
                    profile.driver_instances[profile.driver].setup_writer();
                    that.check_controller_profile_saved(that.edited_controller, that.current_profile, false);
                    that.make_ui();
                })
                lines.push(line_driver);
                
                let driver = profile.driver_instances[profile.driver];
                if (driver) {
                    let driver_lines = driver.make_cofig_inputs();
                    lines = lines.concat(driver_lines);
                    // console.log('Driver config lines ', driver_lines);
                }

                // $('#gamepad-settings-panel').addClass('has-buttons');
            } else {
                // $('#gamepad-settings-panel').removeClass('has-buttons');
            }

            $('#gamepad-profile-config').empty().append(lines);            
        }
    }

    edit_controller(c) {
        this.edited_controller = c;
        console.log('Editing controller '+c.id);
        this.make_ui();
    }

    // collect_profiles() {
    //     Object.values(this.controllers).forEach(c=>{

    //     });
    // }

    make_controller_icons() {
        let icons = [];
        let that = this;

        let types_connected = [];
        Object.values(this.controllers).forEach((c)=>{
            let icon = $('<span class="'+c.type+'"></span>');
            types_connected.push(c.type);
            icons.push(icon);
            icon.click((ev)=>{
                that.edit_controller(c);
                icon.addClass('editing');
            })

            c.icon = icon;
            c.icon_editing = false;
            c.icon_enabled = false;
            c.icon_transmitting = false;

            that.update_controller_icon(c);
        });

        this.update_input_status_icon();

        // tease other controller types (blurred)
        if (types_connected.indexOf('touch') < 0 && isTouchDevice())
            icons.push($('<span class="touch disabled"></span>'));
        if (types_connected.indexOf('keyboard') < 0)
            icons.push($('<span class="keyboard disabled"></span>'));
        if (types_connected.indexOf('gamepad') < 0)
            icons.push($('<span class="gamepad disabled"></span>'));


        $('#input-controller-selection')
            .empty().append(icons);
    }

    update_controller_icon(c) {
        if (!c.icon)
            return;

        if (c.enabled && c.connected && !c.icon_enabled) {
            c.icon_enabled = true;
            c.icon.addClass('enabled');
        } else if ((!c.enabled || !c.connected) && c.icon_enabled) {
            c.icon_enabled = false;
            c.icon.removeClass('enabled');
        }

        if (c.transmitted_last_frame && !c.icon_transmitting) {
            c.icon_transmitting = true;
            c.icon.addClass('transmitting');
        } else if (!c.transmitted_last_frame && c.icon_transmitting) {
            c.icon_transmitting = false;
            c.icon.removeClass('transmitting');
        }

        if (c == this.edited_controller && !c.icon_editing) {
            c.icon_editing = true;
            c.icon.addClass('editing');
        } else if (c != this.edited_controller && c.icon_editing) {
            c.icon_editing = false;
            c.icon.removeClass('editing');
        }
    }

    update_input_status_icon() {
        let something_enabled = false;
        let something_transmitting = false;
        Object.values(this.controllers).forEach((c)=>{
            if (c.icon_enabled)
                something_enabled = true;
            if (c.icon_transmitting)
                something_transmitting = true;
        });

        if (something_enabled && !this.input_status_icon_enabled) {
            this.input_status_icon_enabled = true;
            this.input_status_icon.addClass('enabled');
        } else if (!something_enabled && this.input_status_icon_enabled) {
            this.input_status_icon_enabled = false;
            this.input_status_icon.removeClass('enabled');
        }

        if (something_transmitting && !this.input_status_icon_transmitting) {
            this.input_status_icon_transmitting = true;
            this.input_status_icon.addClass('transmitting');
        } else if (!something_transmitting && this.input_status_icon_transmitting) {
            this.input_status_icon_transmitting = false;
            this.input_status_icon.removeClass('transmitting');
        }
    }


    make_ui() {

        let that = this;

        this.make_profile_selector_ui();

        if (!this.edited_controller) { // autoselect first controller
            let controller_keys = Object.keys(this.controllers);
            this.edited_controller = this.controllers[controller_keys[0]]; 
        }

        this.make_controller_icons();

        this.make_controller_config_ui();

        if (!this.edited_controller || !this.enabled_drivers || !this.current_profile) {
            $('#gamepad-axes-panel').html('Waiting for controllers...');    
            this.debug_output_panel.html('{}');
            // $('#gamepad-profile-config').css('display', 'none');
            this.controller_enabled_cb.attr('disabled', true);
            $('#gamepad-profile-unsaved-warn').css('display', 'none');
            $('#save-gamepad-profile').addClass('saved');
            return;
        }

        // $('#gamepad').addClass('connected');
        console.log('Editing controller is ', this.edited_controller);

        this.controller_enabled_cb
            .attr('disabled', false)
            .prop('checked', this.edited_controller.enabled);

        // gamepad name

        let profile = this.edited_controller.profiles[this.current_profile];

        if (profile.saved && profile.basics_saved) {
            $('#gamepad-profile-unsaved-warn').css('display', 'none');
            $('#save-gamepad-profile').addClass('saved');
        } else {
            $('#gamepad-profile-unsaved-warn').css('display', 'block');
            $('#save-gamepad-profile').removeClass('saved');
        }

        let driver = profile.driver_instances[profile.driver];

        if (this.edited_controller.type == 'keyboard') {
            $('#gamepad-buttons-tab').html('Key mapping');
            $('#gamepad-axes-tab').css('display', 'none'); // no separate axes for kb
            $('#gamepad-axes-panel').css('display', 'none');
            if ($('#gamepad-axes-panel').hasClass('active')) { // switch to buttons tab
                $('#gamepad-axes-panel').removeClass('active');
                $('#gamepad-axes-tab').removeClass('active');
                $('#gamepad-buttons-panel').addClass('active');
                $('#gamepad-buttons-tab').addClass('active');
            }
            this.make_buttons_ui(driver);
        } else {
            // if (this.edited_controller.type == 'touch')
            //     $('#gamepad-buttons-tab').html('UI buttons');
            // else
            $('#gamepad-buttons-tab').html('Buttons');
            $('#gamepad-axes-tab').css('display', ''); //unset
            $('#gamepad-axes-panel').css('display', '');
            this.make_axes_ui(driver);
            this.make_buttons_ui(driver);
        }
    }

    prevent_context_menu(ev) {
        console.log('contex cancelled', ev.target);
        ev.preventDefault();
        ev.stopPropagation();
    };

    // render_assign_button_input(driver, axis) {
    //     let btn_el = $('<div class="config-row"><span class="label">Trigger button:</span></div>');
    //     let btn_inp = $('<span class="btn-input">'+(axis.src_label?axis.src_label:'n/a')+'</span>');
    //     if (axis.id_src!==null)
    //         btn_inp.addClass('assigned');
    
    //     // btn_inp.html('N/A');

    //     let close_listening = () => {
    //         btn_inp.removeClass('listening');
    //         btn_inp.html('listening...');
    //         btn_inp.html(axis.src_label!==null?axis.src_label:'n/a');
    //         if (axis.id_src!==null)
    //             btn_inp.addClass('assigned');
    //         $('#input-manager-overlay').unbind().css('display', 'none');
    //     }
    //     btn_inp.click(()=>{
    //         if (!btn_inp.hasClass('listening')) {
    //             btn_inp.removeClass('assigned').addClass('listening');
    //             btn_inp.html('Press key...');
    //             driver.on_button_press = (btn_id) => {
    //                 axis.id_src = btn_id;
    //                 axis.src_label = 'B'+btn_id;
    //                 console.log('assigned ', btn_id);
    //                 close_listening();
    //                 delete driver.on_button_press;
    //             };
    //             $('#input-manager-overlay').unbind().css('display', 'block').click(()=>{
    //                 close_listening();
    //             });
    //         } else {
    //             close_listening();
    //         }
    //     });
    //     // btn_inp.focus((ev)=>{ev.target.select();});
    //     // offset_inp.change((ev)=>{
    //     //     axis.offset = parseFloat($(ev.target).val());
    //     //     that.check_controller_profile_saved(that.edited_controller, that.current_profile);
    //     // });
    //     btn_inp.appendTo(btn_el);
    //     return btn_el;
    // }

    render_axis_config (driver, axis, button_source = null) {
    
        let that = this;

        axis.config_details_el.empty();
        let driver_axis = axis.driver_axis;

        if (!driver_axis) {
            axis.conf_toggle_el.removeClass('open')
            axis.config_details_el.removeClass('open')
            return; 
        }

        // input offset
        // if (button_source) {
        //     axis.config_details_el.append(this.render_assign_button_input(driver, axis));
        // }
        
        // let default_axis_conf = this.current_gamepad.current_profile[driver_axis];

        // dead zone
        let dead_zone_el = $('<div class="config-row"><span class="label">Dead zone:</span></div>');
        let dead_zone_wrapper_el = $('<div class="config-row2"></div>');
        let dead_zone_min_inp = $('<input type="text" class="inp-val inp-val2"/>');
        
        dead_zone_min_inp.val(axis.dead_min.toFixed(2));
        let dead_zone_max_label = $('<span class="label2">to</span>');
        let dead_zone_max_inp = $('<input type="text" class="inp-val"/>');
        dead_zone_max_inp.val(axis.dead_max.toFixed(2));
        dead_zone_min_inp.appendTo(dead_zone_wrapper_el);
        dead_zone_max_label.appendTo(dead_zone_wrapper_el);
        dead_zone_max_inp.appendTo(dead_zone_wrapper_el);
        dead_zone_wrapper_el.appendTo(dead_zone_el)

        dead_zone_min_inp.focus((ev)=>{ev.target.select();});
        dead_zone_max_inp.focus((ev)=>{ev.target.select();});

        dead_zone_min_inp.change((ev)=>{
            axis.dead_min = parseFloat($(ev.target).val());
            delete axis.dead_val;
            that.check_controller_profile_saved(that.edited_controller, that.current_profile);
        });
        dead_zone_max_inp.change((ev)=>{
            axis.dead_max = parseFloat($(ev.target).val());
            delete axis.dead_val;
            that.check_controller_profile_saved(that.edited_controller, that.current_profile);
        });

        dead_zone_el.appendTo(axis.config_details_el);

        // input offset
        let offset_el = $('<div class="config-row"><span class="label">Offset input:</span></div>');
        let offset_inp = $('<input type="text" class="inp-val"/>');
        offset_inp.val(axis.offset.toFixed(1));
        offset_inp.focus((ev)=>{ev.target.select();});
        offset_inp.change((ev)=>{
            axis.offset = parseFloat($(ev.target).val());
            that.check_controller_profile_saved(that.edited_controller, that.current_profile);
        });
        offset_inp.appendTo(offset_el);
        offset_el.appendTo(axis.config_details_el);

        // input scale
        let scale_el = $('<div class="config-row"><span class="label">Scale input:</span></div>');
        let scale_inp = $('<input type="text" class="inp-val"/>');
        scale_inp.val(axis.scale.toFixed(1));
        scale_inp.focus((ev)=>{ev.target.select();});
        scale_inp.change((ev)=>{
            axis.scale = parseFloat($(ev.target).val());
            that.check_controller_profile_saved(that.edited_controller, that.current_profile);
        });
        scale_inp.appendTo(scale_el);
        scale_el.appendTo(axis.config_details_el);

        // modifier selection
        let mod_func_el = $('<div class="config-row"><span class="label">Modifier:</span></div>');
        let mod_func_opts = [ '<option value="">None</option>' ];
        mod_func_opts.push('<option value="scale_by_velocity" '+(axis.mod_func=='scale_by_velocity'?' selected':'')+'>Scale by velocity</option>');  
        let mod_func_inp = $('<select>'+mod_func_opts.join('')+'</select>');
        mod_func_inp.appendTo(mod_func_el);
        mod_func_el.appendTo(axis.config_details_el);
        let mod_func_cont = $('<div></div>');
        mod_func_cont.appendTo(axis.config_details_el);
        
        let set_mod_funct = (mod_func) => {
            if (mod_func) {
                axis.mod_func = mod_func;
                let mod_func_config_els = [];
                if (mod_func == 'scale_by_velocity') {

                    let multiply_lerp_input_el = $('<div class="config-row"><span class="label sublabel">Velocity source:</span></div>');
                    let multiply_lerp_input_opts = [ '<option value="">Select axis</option>' ];

                    let dri_axes = driver.get_axes();
                    let dri_axes_ids = Object.keys(dri_axes);
                    for (let j = 0; j < dri_axes_ids.length; j++) {
                        let id_axis = dri_axes_ids[j];
                        multiply_lerp_input_opts.push('<option value="'+id_axis+'"' + (axis.scale_by_velocity_src == id_axis ? ' selected':'') +'>'+dri_axes[id_axis]+'</option>');
                    }
                    
                    let multiply_lerp_input_inp = $('<select>'+multiply_lerp_input_opts.join('')+'</select>');
                    multiply_lerp_input_inp.appendTo(multiply_lerp_input_el);
                    mod_func_config_els.push(multiply_lerp_input_el);
                    multiply_lerp_input_inp.change((ev)=>{
                        axis.scale_by_velocity_src = $(ev.target).val();
                        that.check_controller_profile_saved(that.edited_controller, that.current_profile);
                    });
                    
                    // multiplier min
                    let multiply_lerp_min_el = $('<div class="config-row"><span class="label sublabel">Slow multiplier:</span></div>');
                    let multiply_lerp_min_inp = $('<input type="text" class="inp-val"/>');
                    multiply_lerp_min_inp.focus((ev)=>{ev.target.select();});
                    if (axis.scale_by_velocity_mult_min === undefined)
                        axis.scale_by_velocity_mult_min = 1.0;
                    multiply_lerp_min_inp.val(axis.scale_by_velocity_mult_min.toFixed(1));
                    multiply_lerp_min_inp.change((ev)=>{
                        axis.scale_by_velocity_mult_min = parseFloat($(ev.target).val());
                        that.check_controller_profile_saved(that.edited_controller, that.current_profile);
                    });
                    multiply_lerp_min_inp.appendTo(multiply_lerp_min_el);
                    mod_func_config_els.push(multiply_lerp_min_el);
                    

                    // multiplier max
                    let multiply_lerp_max_el = $('<div class="config-row"><span class="label sublabel">Fast multiplier:</span></div>');
                    let multiply_lerp_max_inp = $('<input type="text" class="inp-val"/>');
                    multiply_lerp_max_inp.focus((ev)=>{ev.target.select();});
                    if (axis.scale_by_velocity_mult_max === undefined)
                        axis.scale_by_velocity_mult_max = 1.0;
                    multiply_lerp_max_inp.val(axis.scale_by_velocity_mult_max.toFixed(1));
                    multiply_lerp_max_inp.change((ev)=>{
                        axis.scale_by_velocity_mult_max = parseFloat($(ev.target).val());
                        that.check_controller_profile_saved(that.edited_controller, that.current_profile);
                    });
                    multiply_lerp_max_inp.appendTo(multiply_lerp_max_el);
                    mod_func_config_els.push(multiply_lerp_max_el);

                    if (!isIOS()) { // ios can't do numberic keyboard with decimal and minus signs => so default it is
                        multiply_lerp_min_inp.attr('inputmode', 'numeric');
                        multiply_lerp_max_inp.attr('inputmode', 'numeric');
                    }
                    multiply_lerp_min_inp.on('contextmenu', that.prevent_context_menu);
                    multiply_lerp_max_inp.on('contextmenu', that.prevent_context_menu);

                }
                mod_func_cont.empty().append(mod_func_config_els).css('display', 'block');
            } else {
                delete axis.mod_func; // check_controller_profile_saved expecs undefined (not null)
                mod_func_cont.empty().css('display', 'none');
            }
            
        }
        set_mod_funct(axis.mod_func);
        mod_func_inp.change((ev)=>{
            set_mod_funct($(ev.target).val());
            that.check_controller_profile_saved(that.edited_controller, that.current_profile);
        });
        

        if (!isIOS()) { // ios can't do numberic keyboard with decimal and minus signs => so default it is
            dead_zone_min_inp.attr('inputmode', 'numeric');
            dead_zone_max_inp.attr('inputmode', 'numeric');
            offset_inp.attr('inputmode', 'numeric');
            scale_inp.attr('inputmode', 'numeric');
        }

        dead_zone_min_inp.on('contextmenu', that.prevent_context_menu);
        dead_zone_max_inp.on('contextmenu', that.prevent_context_menu);
        offset_inp.on('contextmenu', that.prevent_context_menu);
        scale_inp.on('contextmenu', that.prevent_context_menu);

    } 

    make_btn_trigger_sel (btn) {
        // modifier selection
        let trigger_el = $('<div class="config-row"><span class="label">Trigger:</span></div>');
        let trigger_opts = [];
        if (this.edited_controller.type == 'gamepad') // gamepad has touch
            trigger_opts.push('<option value="0"'+(btn.trigger===0?' selected':'')+'>Touch</option>');
        trigger_opts.push('<option value="1"'+(btn.trigger===1?' selected':'')+'>Press</option>');
        if (!btn.driver_btn) // driver btn only trigerred by touch or press
            trigger_opts.push('<option value="2"'+(btn.trigger===2?' selected':'')+'>Release</option>')

        let trigger_inp = $('<select'+(trigger_opts.length < 2 ? ' disabled' : '')+'>'+trigger_opts.join('')+'</select>');
        trigger_inp.appendTo(trigger_el);
        let that = this;
        trigger_inp.change((ev)=>{
            // set_mod_funct();
            btn.trigger = parseInt($(ev.target).val())
            that.check_controller_profile_saved(that.edited_controller, that.current_profile);
        });
        return trigger_el;
    }

    render_driver_button_config (driver, btn) {
    
        let that = this;

        btn.config_details_el.empty();
        // let driver_axis = axis.driver_axis;

        if (!btn.driver_btn) {
           
        }

        // btn.config_details_el.append(this.render_assign_button_input(driver, btn));

        btn.config_details_el.append(this.make_btn_trigger_sel(btn));
    }

    render_ros_srv_button_config (driver, btn) {
    
        let that = this;

        btn.config_details_el.empty();
        // let driver_axis = axis.driver_axis;

        // btn.config_details_el.append(this.render_assign_button_input(driver, btn));

        btn.config_details_el.append(this.make_btn_trigger_sel(btn));

        let srv_el = $('<div class="config-row"><span class="label">Service:</span></div>');
        let srv_opts = [
            '<option value="">Select service...</option>'
        ];
        
        // TODO: would be nice to reload this on introspection update when already rendered
        Object.values(this.client.discovered_nodes).forEach((node)=>{
            let service_ids = Object.keys(node.services);
            if (!service_ids.length)
                return;
            
            let node_opts = [];
            service_ids.forEach((id_srv)=>{
                let msg_type = node.services[id_srv].msg_types[0];
                if (that.ui.ignored_service_types.includes(msg_type))
                    return; // not rendering ignored

                node_opts.push('<option value="'+id_srv+':'+msg_type+'"'+(btn.ros_srv_id == id_srv?' selected':'')+'>'+id_srv+'</option>');
            });

            if (node_opts.length) {
                srv_opts.push('<optgroup label="'+node.node+'"></optgroup>');   
                srv_opts = srv_opts.concat(node_opts);
                srv_opts.push('</optgroup>');
            }
        });
        
        let srv_inp = $('<select>'+srv_opts.join('')+'</select>');
        srv_inp.appendTo(srv_el);
        
        let srv_details_el = $('<div></div>');
        let rener_srv_details = () => {
            srv_details_el.empty();
            if (btn.ros_srv_msg_type) {
                srv_details_el.append($('<div class="config-row">' +
                                        '<span class="label">Message type:</span>' +
                                        '<span class="static_val">' + btn.ros_srv_msg_type + '</span>' +
                                        '</div>'));
                
                let srv_val_el = $('<div class="config-row"><span class="label">Send value:</span></div>');

                if (that.ui.input_widgets[btn.ros_srv_msg_type]) {
                    let srv_val_inp = that.ui.input_widgets[btn.ros_srv_msg_type].MakeInputConfigControls(btn, (val)=>{
                        console.log('btn service val set to ', val);
                        btn.ros_srv_val = val;
                        that.check_controller_profile_saved(that.edited_controller, that.current_profile);
                    });
                    srv_val_el.append(srv_val_inp);
                } else {
                    let srv_val_inp = $('<textarea>{json}</textarea>');
                    srv_val_el.append(srv_val_inp);
                    srv_val_el.append($('<div class="cleaner"></div>'));
                }

                srv_details_el.append(srv_val_el);
            }
        };
        rener_srv_details();

        srv_inp.change((ev)=>{
            let val = $(ev.target).val();
            if (val) {
                let vals = val.split(':');
                btn.ros_srv_id = vals[0];
                btn.ros_srv_msg_type = vals[1];
            } else {
                btn.ros_srv_id = null;
                btn.ros_srv_msg_type = null;
            }
            console.log('btn set to ros rv '+btn.ros_srv_id+' msg type='+btn.ros_srv_msg_type);
            rener_srv_details();
            that.check_controller_profile_saved(that.edited_controller, that.current_profile);
        });

        btn.config_details_el.append(srv_el);
        btn.config_details_el.append(srv_details_el);
    }

    render_ctrl_enabled_button_config (driver, btn) {
    
        let that = this;

        btn.config_details_el.empty();
        // let driver_axis = axis.driver_axis;

        // btn.config_details_el.append(this.render_assign_button_input(driver, btn));

        btn.config_details_el.append(this.make_btn_trigger_sel(btn));

        // modifier selection
        let state_el = $('<div class="config-row"><span class="label">Set state:</span></div>');
        let state_opts = [
            '<option value="2"'+(btn.set_ctrl_state===2?' selected':'')+'>Toggle</option>',
            '<option value="1"'+(btn.set_ctrl_state===1?' selected':'')+'>Enabled</option>',
            '<option value="0"'+(btn.set_ctrl_state===0?' selected':'')+'>Disabled</option>',
        ];

        let state_inp = $('<select>'+state_opts.join('')+'</select>');
        state_inp.appendTo(state_el);
        
        state_inp.change((ev)=>{
            // set_mod_funct();
            btn.set_ctrl_state = parseInt($(ev.target).val())
            that.check_controller_profile_saved(that.edited_controller, that.current_profile);
        });

        btn.config_details_el.append(state_el);
    }

    render_input_profile_button_config (driver, btn) {
    
        let that = this;

        btn.config_details_el.empty();
      
        btn.config_details_el.append(this.make_btn_trigger_sel(btn));

        // profile selection
        let profile_el = $('<div class="config-row"><span class="label">Set profile:</span></div>');
        let profile_opts = [];
        Object.keys(this.profiles).forEach((id_profile)=>{
            let label = this.profiles[id_profile].label ? this.profiles[id_profile].label : id_profile;
            profile_opts.push('<option value="'+id_profile+'"'+(btn.set_ctrl_profile==id_profile?' selected':'')+'>'+label+'</option>')
        })
        let profile_inp = $('<select>'+profile_opts.join('')+'</select>');
        profile_inp.appendTo(profile_el);
        
        profile_inp.change((ev)=>{
            // set_mod_funct();
            btn.set_ctrl_profile = $(ev.target).val();
            that.check_controller_profile_saved(that.edited_controller, that.current_profile);
        });

        btn.config_details_el.append(profile_el);
    }

    render_ui_profile_button_config (driver, btn) {
    
        let that = this;

        btn.config_details_el.empty();
      
        btn.config_details_el.append(this.make_btn_trigger_sel(btn));

        // profile selection
        let profile_el = $('<div class="config-row"><span class="label">Set profile:</span></div>');
        let profile_opts = [];
        [].forEach((id_profile)=>{ // TODO
            let label = this.profiles[id_profile].label ? this.profiles[id_profile].label : id_profile;
            profile_opts.push('<option value="'+id_profile+'"'+(btn.set_ui_profile==id_profile?' selected':'')+'>'+label+'</option>')
        })
        profile_opts.push('<option>N/A (yet)</option>')
        let profile_inp = $('<select disabled>'+profile_opts.join('')+'</select>');
        profile_inp.appendTo(profile_el);
        
        profile_inp.change((ev)=>{
            // set_mod_funct();
            btn.set_ui_profile = $(ev.target).val();
            that.check_controller_profile_saved(that.edited_controller, that.current_profile);
        });

        btn.config_details_el.append(profile_el);
    }

    make_axes_ui(driver) {
        
        let that = this;
        let profile = this.edited_controller.profiles[this.current_profile];

        // all gamepad axes
        let axes_els = [];
        for (let i_axis = 0; i_axis < driver.axes.length; i_axis++) {

            let axis = driver.axes[i_axis];

            let row_el = $('<div class="axis-row unused"></div>');

            // raw val
            let raw_val_el = $('<span class="axis-val" title="Raw axis value"></span>');
            raw_val_el.appendTo(row_el);
            axis.raw_val_el = raw_val_el;

            // 1st line
            let line_1_el = $('<div class="axis-config"></div>');

            // axis assignment selection
            let opts = [ '<option value="">Not in use</option>' ];
            // let dri = profile.driver_instance;
            let dri_axes = driver.get_axes();
            let dri_axes_ids = Object.keys(dri_axes);
            for (let j = 0; j < dri_axes_ids.length; j++) {
                let id_axis = dri_axes_ids[j];
                opts.push('<option value="'+id_axis+'"'+(axis.driver_axis == id_axis ? ' selected' : '')+'>'+dri_axes[id_axis]+'</option>');
            }
            opts.push('<option value="=">Copy from axis...</option>');
            let assignment_sel_el = $('<select>'+opts.join('')+'</select>');
            assignment_sel_el.appendTo(line_1_el);
            axis.assignment_sel_el = assignment_sel_el;

            // output val
            let out_val_el = $('<span class="axis-output-val" title="Axis output">0.00</span>');
            out_val_el.appendTo(line_1_el);
            axis.out_val_el = out_val_el;
            
            // config toggle
            axis.conf_toggle_el = $('<span class="conf-toggle'+(axis.edit_open?' open' : '')+'"></span>');
            axis.conf_toggle_el.click((ev)=>{
                if (!axis.conf_toggle_el.hasClass('open')) {
                    axis.conf_toggle_el.addClass('open')
                    axis.config_details_el.addClass('open')
                    axis.edit_open = true;
                } else {
                    axis.conf_toggle_el.removeClass('open')
                    axis.config_details_el.removeClass('open')
                    axis.edit_open = false;
                }
            });
            out_val_el.click((ev)=>{
                axis.conf_toggle_el.click(); // because this happens a lot
            });
            axis.conf_toggle_el.appendTo(line_1_el);

            // collapsable details
            axis.config_details_el = $('<div class="axis-config-details'+(axis.edit_open?' open' : '')+'"></div>');

            // let that = this;
            assignment_sel_el.change((ev)=>{
                let id_axis_assigned = $(ev.target).val();

                console.log('Axis '+axis.i+' assigned to '+id_axis_assigned)

                let cancel_copy = () => {
                    driver.axes.forEach((a)=>{
                        a.raw_val_el
                            .unbind()
                            .removeClass('copy-source');
                        if (profile.copying_into_axis === a) {
                            a.assignment_sel_el.val(''); //set not in use
                            a.row_el
                                .removeClass('copy-destination')
                                .addClass('unused');
                        }
                    });
                    delete profile.copying_into_axis;
                }

                if (id_axis_assigned == '=') {
                    if (profile.copying_into_axis) {
                        // another one is waiting for input, close first
                        cancel_copy();
                    }
                    //console.log('Copy config for axis '+axis.id)
                    profile.copying_into_axis = axis;
                    row_el.addClass('unused copy-destination');

                    // let axes_ids = Object.keys()
                    driver.axes.forEach((a)=>{
                        if (axis == a) //skip self
                            return;
                        a.raw_val_el.addClass('copy-source')
                            .unbind()
                            .click((ev)=>{
                                cancel_copy();
                                console.log('Copying axis '+a.i, a);
                                axis.driver_axis = a.driver_axis;
                                axis.assignment_sel_el.val(a.driver_axis);
                                if (axis.driver_axis) {
                                    axis.dead_min = a.dead_min;
                                    axis.dead_max = a.dead_max;
                                    axis.offset = a.offset;
                                    axis.scale = a.scale;
                                    axis.mod_func = a.mod_func;
                                    axis.scale_by_velocity_src = a.scale_by_velocity_src;
                                    axis.scale_by_velocity_mult_min = a.scale_by_velocity_mult_min;
                                    axis.scale_by_velocity_mult_max = a.scale_by_velocity_mult_max;
                                    that.render_axis_config(driver, axis);
                                    row_el.removeClass('unused');
                                } else {
                                    row_el.addClass('unused');
                                }
                                that.check_controller_profile_saved(that.edited_controller, that.current_profile);
                            });
                    });
                    return;
                } else if (profile.copying_into_axis) {
                    cancel_copy();
                }

                if (id_axis_assigned) {
                    axis.driver_axis = id_axis_assigned;
                    
                    that.render_axis_config(driver, axis);
                    row_el.removeClass('unused');

                    axis.conf_toggle_el.addClass('open');
                    axis.config_details_el.addClass('open');

                } else {
                    axis.driver_axis = null;                   
                    
                    that.render_axis_config(driver, axis);
                    row_el.addClass('unused');
                }

                that.check_controller_profile_saved(that.edited_controller, that.current_profile);
            });

            that.render_axis_config(driver, axis);
            // console.log('axis '+i_axis+' assigned to ', axis.driver_axis);
            if (axis.driver_axis) {
                row_el.removeClass('unused');
            } else {
                row_el.addClass('unused');
            }

            line_1_el.appendTo(row_el);
            axis.config_details_el.appendTo(row_el);

            axis.row_el = row_el;
            axes_els.push(row_el);
           
        }

        $('#gamepad-axes-panel')
            .empty()
            .append(axes_els);

        if (driver.axes_scroll_offset !== undefined) {
            $('#gamepad-axes-panel').scrollTop(driver.axes_scroll_offset);
            delete driver.axes_scroll_offset;
        }
    }

    render_btn_config(driver, btn) {
                
        btn.config_details_el.empty();
        
        if (btn.driver_axis) {

            if (btn.dead_min === undefined) btn.dead_min = -0.01;
            if (btn.dead_max === undefined) btn.dead_max = 0.01;
            if (btn.offset === undefined) btn.offset = 0.0;
            if (btn.scale === undefined) btn.scale = 1.0;
            // dead_min: axis_cfg && axis_cfg.dead_min !== undefined ? axis_cfg.dead_min : -default_dead_zone,
            // dead_max: axis_cfg && axis_cfg.dead_max !== undefined ? axis_cfg.dead_max : default_dead_zone,
            // offset: axis_cfg && axis_cfg.offset !== undefined ? axis_cfg.offset : 0.0,
            // scale: axis_cfg && axis_cfg.scale !== undefined ? axis_cfg.scale : 1.0,                

            this.render_axis_config(driver, btn, true); //render button as axis with input for trigger src
        } else if (btn.driver_btn) {

            if (btn.trigger === undefined) btn.trigger = 1; // press by default
            this.render_driver_button_config(driver, btn); //render button as axis

        } else if (btn.action) {

            switch (btn.action) {
                case 'ros-srv':
                    this.render_ros_srv_button_config(driver, btn);
                    break;
                case 'ctrl-enabled': 
                    this.render_ctrl_enabled_button_config(driver, btn);
                    break;
                case 'input-profile': 
                    this.render_input_profile_button_config(driver, btn);
                    break;
                case 'ui-profile': 
                    this.render_ui_profile_button_config(driver, btn);
                    break;
                default: 
                    console.error('Button action type not supported: ', btn.action)
                    btn.conf_toggle_el.removeClass('open');
                    btn.config_details_el.removeClass('open');
                    return; 
            }

        } else {
            btn.conf_toggle_el.removeClass('open')
            btn.config_details_el.removeClass('open')
            return; 
        }
    };

    make_btn_row(driver, btn) {

        let that = this;

        let row_el = $('<div class="button-row unused"></div>');
        btn.row_el = row_el;

        // raw val
        let raw_val_el = $('<span class="btn-val" title="Button input"></span>');
        raw_val_el.appendTo(row_el);

        let close_listening = () => {
            btn.listening = false;
            raw_val_el.removeClass('listening');
            raw_val_el.html(btn.src_label!==null?btn.src_label:'n/a');
            if (btn.id_src!==null)
                raw_val_el.addClass('assigned');
            $('#input-manager-overlay').unbind().css('display', 'none');
            delete driver.on_button_press;
        };
        raw_val_el.click(()=>{
            if (!raw_val_el.hasClass('listening')) {
                raw_val_el.removeClass('assigned').addClass('listening');
                raw_val_el.html('?');
                $('#input-manager-overlay').unbind().css('display', 'block').click(()=>{
                    close_listening();
                });
                btn.listening = true;
                driver.on_button_press = (key_id, kb_ev) => {
                
                    if (that.edited_controller.type == 'keyboard') {

                        if (key_id == 'Escape') { // cancel 
                            close_listening();
                            return;
                        }
                        else if (key_id == 'Delete' || key_id == 'Backspace') { // clear
                            btn.id_src = null;
                            btn.src_label = null;
                            btn.key_mod = null;
                            close_listening();
                            return;
                        }
                        // else if (key_id == )
                        let label = kb_ev.key;
                        switch (label) {
                            case ' ': label = '␣'; break;
                            case 'ArrowLeft': label = '←'; break; // &#8592;
                            case 'ArrowRight': label = '→'; break; //&#8594;
                            case 'ArrowUp': label = '↑'; break; // &#8593;
                            case 'ArrowDown': label = '↓'; break; // &#8595;
                        }
                        if (kb_ev.altKey) {
                            label = 'Alt+'+label;
                            btn.key_mod = 'alt';
                        }
                        else if (kb_ev.ctrlKey) {
                            label = 'Ctrl+'+label;
                            btn.key_mod = 'ctrl';
                        }
                        else if (kb_ev.metaKey) {
                            label = 'Meta+'+label;
                            btn.key_mod = 'meta';
                        }
                        else if (kb_ev.shiftKey) {
                            label = 'Shift+'+label;
                            btn.key_mod = 'shift';
                        } else {
                            btn.key_mod = null;
                        }
                        btn.src_label = label;

                    } else { // touch or gamepad
                        btn.src_label = 'B'+key_id;
                    }
                        
                    btn.id_src = key_id;
                    // console.log('assigned ', key_id);

                    close_listening();
                };
            } else {
                close_listening();
            }
        });


        btn.raw_val_el = raw_val_el;

         // 1st line
        let line_1_el = $('<div class="btn-config"></div>');

        // btn assignment selection
        let opts = [
            '<option value="">Not in use</option>',
            '<option value="ctrl-enabled"'+(btn.action == 'ctrl-enabled' ? ' selected': '')+'>Set Controller Enabled</option>',
            '<option value="ros-srv"'+(btn.action == 'ros-srv' ? ' selected': '')+'>Call ROS Service</option>',
            '<option value="input-profile"'+(btn.action == 'input-profile' ? ' selected': '')+'>Set Input Profile</option>',
            '<option value="ui-profile"'+(btn.action == 'ui-profile' ? ' selected': '')+'>Set UI Layout</option>',
        ];
        // let dri = profile.driver_instance;
        let dri_btns = driver.get_buttons();
        let dri_btns_ids = Object.keys(dri_btns);
        for (let j = 0; j < dri_btns_ids.length; j++) {
            let id_btn = dri_btns_ids[j];
            opts.push('<option value="btn:'+id_btn+'"'+(btn.driver_btn == id_btn ? ' selected' : '')+'>'+dri_btns[id_btn]+'</option>');
        }
        let dri_axes = driver.get_axes();
        let dri_axes_ids = Object.keys(dri_axes);
        for (let j = 0; j < dri_axes_ids.length; j++) {
            let id_axis = dri_axes_ids[j];
            opts.push('<option value="axis:'+id_axis+'"'+(btn.driver_axis == id_axis ? ' selected' : '')+'>'+dri_axes[id_axis]+'</option>');
        }
        opts.push('<option value="=">Copy from button...</option>');
        let assignment_sel_el = $('<select>'+opts.join('')+'</select>');
        assignment_sel_el.appendTo(line_1_el);
        btn.assignment_sel_el = assignment_sel_el;

        // output val
        let out_val_el = $('<span class="btn-output-val" title="Button output"></span>');
        out_val_el.appendTo(line_1_el);
        btn.out_val_el = out_val_el;

        // config toggle
        btn.conf_toggle_el = $('<span class="conf-toggle'+(btn.edit_open?' open' : '')+'"></span>');
        btn.conf_toggle_el.click((ev)=>{
            if (!btn.conf_toggle_el.hasClass('open')) {
                btn.conf_toggle_el.addClass('open')
                btn.config_details_el.addClass('open')
                btn.edit_open = true;
            } else {
                btn.conf_toggle_el.removeClass('open')
                btn.config_details_el.removeClass('open')
                btn.edit_open = false;
            }
        });
        out_val_el.click((ev)=>{
            btn.conf_toggle_el.click(); // because this happens a lot
        });
        btn.conf_toggle_el.appendTo(line_1_el);

        // collapsable details
        btn.config_details_el = $('<div class="btn-config-details'+(btn.edit_open?' open' : '')+'"></div>');

        assignment_sel_el.change((ev)=>{
            let val_btn_assigned = $(ev.target).val();

            // console.log('Btn '+btn.i+' assigned to '+val_btn_assigned)

            // let cancel_copy = () => {
            //     driver.axes.forEach((a)=>{
            //         a.raw_val_el
            //             .unbind()
            //             .removeClass('copy-source');
            //         if (profile.copying_into_axis === a) {
            //             a.assignment_sel_el.val(''); //set not in use
            //             a.row_el
            //                 .removeClass('copy-destination')
            //                 .addClass('unused');
            //         }
            //     });
            //     delete profile.copying_into_axis;
            // }

            // if (id_axis_assigned == '=') {
            //     if (profile.copying_into_axis) {
            //         // another one is waiting for input, close first
            //         cancel_copy();
            //     }
            //     //console.log('Copy config for axis '+axis.id)
            //     profile.copying_into_axis = axis;
            //     row_el.addClass('unused copy-destination');

            //     // let axes_ids = Object.keys()
            //     driver.axes.forEach((a)=>{
            //         if (axis == a) //skip self
            //             return;
            //         a.raw_val_el.addClass('copy-source')
            //             .unbind()
            //             .click((ev)=>{
            //                 cancel_copy();
            //                 console.log('Copying axis '+a.i, a);
            //                 axis.driver_axis = a.driver_axis;
            //                 axis.assignment_sel_el.val(a.driver_axis);
            //                 if (axis.driver_axis) {
            //                     axis.dead_min = a.dead_min;
            //                     axis.dead_max = a.dead_max;
            //                     axis.offset = a.offset;
            //                     axis.scale = a.scale;
            //                     axis.mod_func = a.mod_func;
            //                     axis.scale_by_velocity_src = a.scale_by_velocity_src;
            //                     axis.scale_by_velocity_mult_min = a.scale_by_velocity_mult_min;
            //                     axis.scale_by_velocity_mult_max = a.scale_by_velocity_mult_max;
            //                     render_axis_config();
            //                     row_el.removeClass('unused');
            //                 } else {
            //                     row_el.addClass('unused');
            //                 }
            //                 that.check_controller_profile_saved(that.edited_controller, that.current_profile);
            //             });
            //     });
            //     return;
            // } else if (profile.copying_into_axis) {
            //     cancel_copy();
            // }

            if (val_btn_assigned) {

                if (val_btn_assigned.indexOf('axis:') === 0) {
                    let id_axis_assigned = val_btn_assigned.substring(5);;
                    console.log('btn '+ btn.i +' assigned axis: ', id_axis_assigned)
                    btn.driver_btn = null;
                    btn.action = null;
                    btn.driver_axis = id_axis_assigned;
                    btn.assigned = true;
                } else if (val_btn_assigned.indexOf('btn:') === 0) {
                    let id_btn_assigned = val_btn_assigned.substring(4);;
                    console.log('btn '+ btn.i +' assigned btn: ', id_btn_assigned)
                    btn.driver_axis = null;
                    btn.action = null;
                    btn.driver_btn = id_btn_assigned;
                    if (btn.trigger === undefined || btn.trigger === null)
                        btn.trigger = 1; // press by default
                    btn.assigned = true;
                } else { // actions
                    console.log('btn '+ btn.i +' assigned action: ', val_btn_assigned)
                    btn.driver_axis = null;
                    btn.driver_btn = null;
                    btn.action = val_btn_assigned;
                    if (btn.trigger === undefined || btn.trigger === null)
                        btn.trigger = 1; // press by default
                    switch (btn.action) {
                        case 'ctrl-enabled': 
                            if (btn.set_ctrl_state === undefined || btn.set_ctrl_state === null)
                                btn.set_ctrl_state = 2; //toggle by default
                            
                            break;
                        default: break;
                    }
                    btn.assigned = true;
                }
                
                that.render_btn_config(driver, btn);
                row_el.removeClass('unused');

                btn.conf_toggle_el.addClass('open');
                btn.config_details_el.addClass('open');
                btn.edit_open = true;

            } else {
                btn.driver_axis = null;                   
                btn.driver_btn = null;
                btn.action = null;
                btn.assigned = false;
                btn.edit_open = false;
                
                that.render_btn_config(driver, btn);
                row_el.addClass('unused');
            }

            that.check_controller_profile_saved(that.edited_controller, that.current_profile);
        });

        this.render_btn_config(driver, btn);

        if (btn.driver_btn || btn.driver_axis || btn.action) {
            row_el.removeClass('unused');
        } else {
            row_el.addClass('unused');
        }

        line_1_el.appendTo(row_el);
        btn.config_details_el.appendTo(row_el);

        return row_el;
    }

    make_buttons_ui(driver) {
        
        let that = this;

        // all gamepad axes
        let button_els = [];
        for (let i_btn = 0; i_btn < driver.buttons.length; i_btn++) {
            let btn = driver.buttons[i_btn];
            let row_el = this.make_btn_row(driver, btn);
            button_els.push(row_el);
        }

        let add_btn = null;
        if (this.edited_controller.type == 'keyboard') {
            add_btn = $('<button id="add-button-btn"><span class="icon"></span>Add key mapping</button>')
        }
        else if (this.edited_controller.type == 'touch') {
            add_btn = $('<button id="add-button-btn"><span class="icon"></span>Add UI button</button>')
        }
        else if (this.edited_controller.type == 'gamepad') {
            add_btn = $('<button id="add-button-btn"><span class="icon"></span>Add button</button>')
        }

        if (add_btn) {
            add_btn.click((ev)=>{
                let new_btn = that.make_button(driver);
                let row_el = this.make_btn_row(driver, new_btn);
                row_el.insertBefore($('#add-button-btn'));
                $('#gamepad-buttons-panel').scrollTop($('#gamepad-buttons-panel').prop('scrollHeight'));
            });
            button_els.push(add_btn);
        }

        $('#gamepad-buttons-panel')
            .empty()
            .append(button_els);

        if (driver.buttons_scroll_offset !== undefined) {
            $('#gamepad-buttons-panel').scrollTop(driver.buttons_scroll_offset);
            delete driver.buttons_scroll_offset;
        }
    }

    update_axes_ui_values () {
        if (!this.edited_controller)
            return;
        
        let profile = this.edited_controller.profiles[this.current_profile];
        let driver = profile.driver_instances[profile.driver];

        for (let i_axis = 0; i_axis < driver.axes.length; i_axis++) {

            let axis = driver.axes[i_axis];

            if (axis.raw !== null && axis.raw !== undefined)
                axis.raw_val_el.html(axis.raw.toFixed(2));
            else 
                axis.raw_val_el.html('0.00');

            if (!axis.driver_axis)
                continue;
            
            if (axis.val !== null && axis.val !== undefined)
                axis.out_val_el.html(axis.val.toFixed(2));

            if (axis.live) {
                axis.out_val_el.addClass('live');
            } else {
                axis.out_val_el.removeClass('live');
            }
        }
    }

    update_buttons_ui_values () {
        if (!this.edited_controller)
            return;
        
        let profile = this.edited_controller.profiles[this.current_profile];
        let driver = profile.driver_instances[profile.driver];

        for (let i_btn = 0; i_btn < driver.buttons.length; i_btn++) {

            let btn = driver.buttons[i_btn];
            if (btn.listening)
                continue;

            if (this.edited_controller.type == 'keyboard' && btn.id_src) {

                btn.raw_val_el.html(btn.src_label);

                if (btn.pressed)
                    btn.raw_val_el.addClass('pressed');
                else
                    btn.raw_val_el.removeClass('pressed');

            } else if (Number.isInteger(btn.id_src)) {

                if (btn.driver_axis && (btn.pressed || btn.touched) && !(btn.raw === undefined || btn.raw === null)) {
                    btn.raw_val_el.html(btn.raw.toFixed(2));
                } else if (btn.src_label) {
                    btn.raw_val_el.html(btn.src_label);
                } else {
                    btn.raw_val_el.html('none');
                }

                if (btn.touched)
                    btn.raw_val_el.addClass('touched');
                else
                    btn.raw_val_el.removeClass('touched');
    
                if (btn.pressed)
                    btn.raw_val_el.addClass('pressed');
                else
                    btn.raw_val_el.removeClass('pressed');

            } else {
                btn.raw_val_el.removeClass('touched');
                btn.raw_val_el.removeClass('pressed');
                btn.raw_val_el.html('none');
            }

            if (btn.assigned) {

                if (btn.driver_btn && (btn.val === true || btn.val === false))
                    btn.out_val_el.html(btn.val.toString());
                else if (btn.driver_axis && btn.val !== null && btn.val !== undefined)
                    btn.out_val_el.html(btn.val.toFixed(2));
                else
                    btn.out_val_el.html('n/a');
    
                if (btn.live) {
                    btn.out_val_el.addClass('live');
                } else {
                    btn.out_val_el.removeClass('live');
                }

            }
            
        }
    }

    process_axes_input(c) {

        let profile = c.profiles[this.current_profile];
        let driver = profile.driver_instances[profile.driver];

        let combined_axes_vals = {}; // 1st pass, same axess added to single val
        let combined_axes_unscaled_vals = {}; // expected to be within [-1; +1] (offset added and scaling sign kept)

        let some_axes_live = false;

        let axes_to_process = [].concat(driver.axes);
        for (let i_btn = 0; i_btn < driver.buttons.length; i_btn++) {
            if (driver.buttons[i_btn].driver_axis) {
                axes_to_process.push(driver.buttons[i_btn]); //button mapped as axis
            }
        }

        for (let i = 0; i < axes_to_process.length; i++) {
            let axis = axes_to_process[i];
           
            if (axis.dead_val === undefined) // unset on min/max change
                axis.dead_val = (axis.dead_min+axis.dead_max) / 2.0;

            if (!axis.driver_axis)
                continue;
        
            let raw = axis.raw;
            if (raw === null || raw === undefined)
                raw = axis.dead_val; //null => assign dead;

            let out = raw; 
            
            let out_unscaled = raw;
            let live = true;
            if (raw > axis.dead_min && raw < axis.dead_max) {
                live = false;
                out = axis.dead_val;
                out_unscaled = axis.dead_val;
            } else {
                out += axis.offset;
                out_unscaled = out;
                if (axis.scale < 0) // sign matters (saving unsaled offset vals as normalized)
                    out_unscaled = -1.0 * out_unscaled;
                out *= axis.scale;
            }

            axis.base_val = out;
            axis.val = out; // modifier might change this in 2nd pass
            axis.live = live;
            
            some_axes_live = some_axes_live || axis.live;

            if (combined_axes_vals[axis.driver_axis] === undefined) {
                combined_axes_vals[axis.driver_axis] = axis.base_val;
                combined_axes_unscaled_vals[axis.driver_axis] = out_unscaled;
            } else { // add multiple axes into one (use this for negative/positive split)
                combined_axes_vals[axis.driver_axis] += axis.base_val;
                combined_axes_unscaled_vals[axis.driver_axis] += out_unscaled;
            }
                
        }

        driver.axes_output = {}; // this goes to the driver

        // 2nd pass - modifiers that use base vals and split-axes added together
        for (let i = 0; i < axes_to_process.length; i++) {
            let axis = axes_to_process[i];

            if (!axis.driver_axis || !axis.live) {
                continue;
            }

            if (!axis.mod_func) {
                if (driver.axes_output[axis.driver_axis] === undefined)
                    driver.axes_output[axis.driver_axis] = axis.val;
                else
                    driver.axes_output[axis.driver_axis] += axis.val;
                continue; // all good
            }

            switch (axis.mod_func) {
                case 'scale_by_velocity':
                    if (!axis.scale_by_velocity_src || combined_axes_unscaled_vals[axis.scale_by_velocity_src] === undefined) {
                        axis.val = axis.dead_val; // hold until fully configured
                        axis.live = false;
                        continue;
                    }
                        
                    let velocity_normalized = combined_axes_unscaled_vals[axis.scale_by_velocity_src];
                    let abs_velocity_normalized = Math.abs(Math.max(-1.0, Math.min(1.0, velocity_normalized))); // clamp abs to [0.0; 1.0]
                    
                    let multiplier = lerp(axis.scale_by_velocity_mult_min, axis.scale_by_velocity_mult_max, abs_velocity_normalized);

                    axis.val *= multiplier;
                    if (driver.axes_output[axis.driver_axis] === undefined)
                        driver.axes_output[axis.driver_axis] = axis.val;
                    else
                        driver.axes_output[axis.driver_axis] += axis.val;

                    // console.log('Scaling axis '+i_axis+' ('+axis.driver_axis+') by '+abs_velocity_normalized+' ('+axis.scale_by_velocity_src+') m='+multiplier)

                    break;
                default:
                    break;
            }

        }

        return some_axes_live;
    }

    process_buttons_input(c) {

        let profile = c.profiles[this.current_profile];
        let driver = profile.driver_instances[profile.driver];

        let some_buttons_live = false;
        driver.buttons_output = {}; // this goes to the drive

        for (let i_btn = 0; i_btn < driver.buttons.length; i_btn++) {
            let btn = driver.buttons[i_btn];
    
            if (!btn.driver_btn)
                continue;
            
            btn.val = false;
            if (btn.trigger === 0 && btn.touched) {
                btn.val = true;
            }
            else if (btn.trigger === 1 && btn.pressed) {
                btn.val = true;
            }
            btn.live = btn.val;
            some_buttons_live = btn.live || some_buttons_live;

            if (driver.buttons_output[btn.driver_btn] === undefined)
                driver.buttons_output[btn.driver_btn] = btn.val;
            else
                driver.buttons_output[btn.driver_btn] = btn.val || driver.buttons_output[btn.driver_btn]; // allow triggering with multiiple btns
        }

        return some_buttons_live;
    }

    on_gamepad_connected(ev) {
            
        let id_gamepad = ev.gamepad.id;

        if (!this.controllers[id_gamepad]) {

            console.warn('Gamepad connected:', id_gamepad, ev.gamepad);
            let gamepad = {
                type: 'gamepad',
                id: id_gamepad,
                gamepad: ev.gamepad,
                profiles: null,
                saved_profiles: null,
                initiated: false, //this will wait for config
                connected: true,
            };
            this.controllers[id_gamepad] = gamepad;
            // this.save_gamepad_was_once_connected();

        } else {
            this.controllers[id_gamepad].gamepad = ev.gamepad;
            this.controllers[id_gamepad].connected = true;
            console.info('Gamepad already connected:', id_gamepad);
        }

        // if (!(this.current_gamepad && this.current_gamepad.isTouch)) {
        //     //touch ui has priority when on
        //     this.current_gamepad = this.connected_gamepads[id_gamepad]; 
        // }

        this.init_controller(this.controllers[id_gamepad]);
    }

    on_gamepad_disconnected (ev) {

        if (this.controllers[ev.gamepad.id]) {

            console.log('Gamepad disconnected '+ev.gamepad.id);
            this.controllers[ev.gamepad.id].gamepad = null;
            this.controllers[ev.gamepad.id].connected = false;

            // if (this.edited_controller.id == ev.gamepad.id) {
            //     // this.edited_controller = null;
            //     this.make_ui();
            // } else {
            this.make_controller_icons();
            // }
        }

    }

    make_touch_gamepad() {
        if (!this.controllers['touch']) {
            let touch_gamepad = {
                type: 'touch',
                id: 'touch',
                profiles: null,
                saved_profiles: null,                
                initiated: false, //this will wait for config
                connected: false,
            };
            this.controllers['touch'] = touch_gamepad;
            this.init_controller(touch_gamepad);
        }
    }

    make_keyboard() {
        if (!this.controllers['keyboard']) {
            let kb = {
                type: 'keyboard',
                id: 'keyboard',
                profiles: null,
                saved_profiles: null,
                initiated: false, //this will wait for config
                connected: true,
            };
            this.controllers['keyboard'] = kb;
            this.init_controller(kb);
        }
    }

    set_touch(state) {
        
        this.touch_gamepad_on = state;
        this.controllers['touch'].connected = state;

        if (state) {

            if (this.edited_controller != this.controllers['touch']) {
                this.edited_controller = this.controllers['touch'];
            }

            this.init_controller(this.controllers['touch']);

        } else {


            this.make_controller_icons();        

            // this.current_gamepad = null; // kills the loop
            // console.log('Gamepad touch mode off')

            // let that = this;
            // Object.values(this.controllers).forEach((c)=>{
            //     if (!c.isTouch && gp.gamepad) { // physical gamepad connected => fall back
            //         console.log('Falling back to '+gp.id);
            //         that.current_gamepad = gp;
            //         return;
            //     }
            // })

            // if (this.current_gamepad) {
            //     this.init_gamepad(this.current_gamepad);
            // } else {
            //     this.make_ui();
            // }

        }
        
    }

    touch_input(where, value, angle) {
        if (value) {
            if (!this.last_touch_input[where]) {
                this.last_touch_input[where] = new THREE.Vector2();
            }
            this.last_touch_input[where].set(value, 0);
            this.last_touch_input[where].rotateAround(this.zero, angle);
        } else {
            delete this.last_touch_input[where];
        }
    }

    run_loop() {

        if (!this.loop_running) {
            console.log('Gamepad loop stopped')
            this.loop_running = false;
            return;
        }

        let that = this;
        Object.values(this.controllers).forEach((c)=>{
        
            if (!c.profiles)
                return; //not yet configured

            let profile = c.profiles[that.current_profile];
            let driver = profile.driver_instances[profile.driver];
    
            if (c.type == 'touch') {
    
                if (this.last_touch_input['left']) {
                    driver.axes[0].raw = this.last_touch_input['left'].x;
                    driver.axes[1].raw = this.last_touch_input['left'].y;
                } else {
                    driver.axes[0].raw = 0.0;
                    driver.axes[1].raw = 0.0;
                }
                if (this.last_touch_input['right']) {
                    driver.axes[2].raw = this.last_touch_input['right'].x;
                    driver.axes[3].raw = this.last_touch_input['right'].y;
                } else {
                    driver.axes[2].raw = 0.0;
                    driver.axes[3].raw = 0.0;
                }

            } else if (c.type == 'keyboard') {

               // handle in on_keyboard_key_down/up

            } else if (c.type == 'gamepad') {
                
                if (c.gamepad) {
                    try {
                        let gps = navigator.getGamepads();
                        let gp = c.gamepad ? gps[c.gamepad.index] : null;
                        if (!gp) {
                            return;
                        }
                        for (let i = 0; i < gp.axes.length; i++) {
                            let read_val = gp.axes[i];
                            if (driver.axes[i].needs_reset) {  
                                if (read_val != 0.0) { // wait for first non-zero signal because some gamepads are weird
                                    delete driver.axes[i].needs_reset;
                                    driver.axes[i].raw = read_val;
                                    // console.log('GP axis '+i+'; reset val='+read_val)
                                } else {
                                    driver.axes[i].raw = null;
                                }
                            } else {
                                driver.axes[i].raw = read_val;
                            }
                        }
                        // for (let i = 0; i < gp.buttons.length; i++) {
                        
                        if (driver.on_button_press) {
                            for (let i = 0; i < gp.buttons.length; i++) {
                                if (gp.buttons[i].pressed) {
                                    driver.on_button_press(i);
                                }
                            }   
                        } else {
                            for (let j = 0; j < driver.buttons.length; j++) {
                                let btn = driver.buttons[j];
                                if (btn.id_src === null)
                                    continue;
    
                                let gp_btn = gp.buttons[btn.id_src];
                                if (!btn)
                                    continue;
    
                                let read_val = gp_btn.value;
                                if (btn.needs_reset) {  
                                    if (read_val != 0.0) { // wait for first non-zero signal because some gamepads are weird
                                        delete btn.needs_reset;
                                        btn.raw = read_val;
                                        btn.pressed = gp_btn.pressed;
                                        btn.touched = gp_btn.touched;
                                        // console.log('GP axis '+i+'; reset val='+read_val)
                                    } else {
                                        btn.raw = null;
                                        btn.pressed = false;
                                        btn.touched = false;
                                    }
                                } else {
                                    btn.raw = read_val;
                                    btn.pressed = gp_btn.pressed;
                                    btn.touched = gp_btn.touched;
                                }
                                
                            }
                        }
                            
                        // }
    
                    } catch (e) {
                        console.error('Error reading gp axes; c.gp=', c.gamepad);
                        console.log(e);
                        return;
                    }
                } else {
                    for (let i = 0; i < driver.axes.length; i++) {
                        driver.axes[i].raw = null;
                        driver.axes[i].needs_reset = true;
                    }
                    for (let i = 0; i < driver.buttons.length; i++) {
                        driver.buttons[i].raw = null;
                        driver.buttons[i].needs_reset = true;
                        driver.buttons[i].pressed = false;
                        driver.buttons[i].touched = false;
                    }
                }
            } else
                return; //nothing to do for this controller atm
    
            let axes_alive = this.process_axes_input(c) && c.connected;
            let buttons_alive = this.process_buttons_input(c) && c.connected;
            let cooldown = false;

            if (!axes_alive && !buttons_alive && c.transmitted_last_frame) { // cooldown for 1s to make sure zero values are received
                if (c.cooldown_started === undefined) {
                    c.cooldown_started = Date.now();
                    cooldown = true;
                } else if (c.cooldown_started + 1000 > Date.now() ) {
                    cooldown = true;
                }
            } else if (c.cooldown_started !== undefined) {
                delete c.cooldown_started; // some axes alive => reset cooldown
            }
    
            let transmitting = c.enabled && (axes_alive || buttons_alive || cooldown) && driver.can_transmit();
    
            driver.generate();
            
            if (this.edited_controller == c) {
                driver.display_output(this.debug_output_panel, transmitting);
            }
    
            c.transmitted_last_frame = transmitting;

            if (transmitting) {
                driver.transmit();
            }

            that.update_controller_icon(c);
            that.update_input_status_icon();

        });

        this.update_axes_ui_values();
        this.update_buttons_ui_values();

        // if (transmitting && !this.transmitting) {
        //     this.transmitting = true;
        //     this.status_icon.addClass('transmitting');
        // } else if (!transmitting && this.transmitting) {
        //     this.transmitting = false;
        //     this.status_icon.removeClass('transmitting');
        // }
  
        return window.setTimeout(
            () => { this.run_loop(); },
            this.loop_delay
        );
    }



    on_keyboard_key_down(ev, c) {
        // console.log('Down: '+ev.code, ev);

        let profile = c.profiles[this.current_profile];
        let driver = profile.driver_instances[profile.driver];

        if (driver.on_button_press) {
            if (['Shift', 'Control', 'Alt', 'Meta'].indexOf(ev.key) > -1) {
                return; // ignore single modifiers here
            }
            driver.on_button_press(ev.code, ev);
            return;
        }

        for (let i = 0; i < driver.buttons.length; i++) {
            let btn = driver.buttons[i];
            if (btn.pressed)
                continue;

            if (btn.id_src == ev.code) {
                if (btn.key_mod == 'shift' && !ev.shiftKey)
                    continue;
                if (btn.key_mod == 'meta' && !ev.metaKey)
                    continue;
                if (btn.key_mod == 'ctrl' && !ev.ctrlKey)
                    continue;
                if (btn.key_mod == 't' && !ev.altKey)
                    continue;

                btn.pressed = true;
                btn.raw = 1.0;

                //TODO down handlers go here
            }
        }
    }

    on_keyboard_key_up(ev, c) {
        // console.log('Up: '+ev.code, ev);

        let profile = c.profiles[this.current_profile];
        let driver = profile.driver_instances[profile.driver];

        if (driver.on_button_press) { // if still listening, thus must be a single modifier
            driver.on_button_press(ev.code, ev); 
            return; //assign
        }

        for (let i = 0; i < driver.buttons.length; i++) {
            let btn = driver.buttons[i];
            if (!btn.pressed)
                continue;

            if (btn.id_src == ev.code || 
                (ev.key == 'Shift' && btn.key_mod == 'shift') ||
                (ev.key == 'Alt' && btn.key_mod == 'alt') ||
                (ev.key == 'Ctrl' && btn.key_mod == 'ctrl') ||
                (ev.key == 'Meta' && btn.key_mod == 'meta')
            ) {
                btn.pressed = false;
                btn.raw = 0.0;

                //TODO up handlers go here
            }
        }
    }


    save_user_controller_enabled(c) {
        if (c.type == 'touch')
            return; // touch ui starts always on

        localStorage.setItem('controller-enabled:' + this.client.id_robot+ ':' + c.id,
                            c.enabled);
        console.log('Saved controller enabled for robot '+this.client.id_robot+', gamepad "'+c.id+'":', c.enabled);
    }

    load_user_controller_enabled(id_controller) {
        let state = localStorage.getItem('controller-enabled:' + this.client.id_robot + ':' + id_controller);
        state = state === 'true';
        console.log('Loaded controller enabled for robot '+this.client.id_robot+', gamepad "'+id_controller+'":', state);
        return state;
    }

    // save_user_gamepad_driver() {
    //     if (!this.gamepad)
    //         return; // saving per gp

    //     localStorage.setItem('gamepad-dri:' + this.client.id_robot
    //                         + ':' + this.gamepad.id,
    //                         this.current_driver.id);
    //     console.log('Saved gamepad driver for robot '+this.client.id_robot+', gamepad "'+this.gamepad.id+'":', this.current_driver.id);
    // }

    // load_user_gamepad_driver(id_gamepad) {
    //     let dri = localStorage.getItem('gamepad-dri:' + this.client.id_robot
    //                                     + ':' + id_gamepad);
    //     console.log('Loaded gamepad driver for robot '+this.client.id_robot+', gamepad "'+id_gamepad+'":', dri);
    //     return dri;
    // }

    // save_user_driver_config() {
    //     localStorage.setItem('gamepad-cfg:' + this.client.id_robot
    //                             + ':' + this.gamepad.id
    //                             + ':' + this.current_driver.id,
    //                         JSON.stringify(this.current_driver.config));
    //     console.log('Saved gamepad config for robot '+this.client.id_robot+', gamepad "'+this.gamepad.id+'", driver '+this.current_driver.id+':', this.current_driver.config);
    // }

    // load_user_driver_config(id_gamepad, id_driver) {
    //     let cfg = localStorage.getItem('gamepad-cfg:' + this.client.id_robot
    //                             + ':' + id_gamepad
    //                             + ':' + id_driver);

    //     if (cfg) {
    //         try {
    //             cfg = JSON.parse(cfg);
    //         }
    //         catch {
    //             cfg = null;
    //         }
    //     }

    //     console.log('Loaded gamepad user config for robot '+this.client.id_robot+', gamepad "'+id_gamepad+'", driver '+id_driver+':', cfg);
    //     return cfg;
    // }

    // set_default_config() {
    //     this.current_driver.config = this.current_driver.default_gamepad_config;
    //     $('#gamepad_config_input').removeClass('err');
    //     this.config_to_editor();
    // }

    // set_default_shortcuts() {
    //     this.shortcuts_config = this.default_shortcuts_config;
    //     $('#gamepad_shortcuts_input').removeClass('err');
    //     this.shortcuts_to_editor();
    // }

    // save_user_shortcuts() {
    //     localStorage.setItem('gamepad-keys:' + this.client.id_robot
    //                             + ':' + this.gamepad.id,
    //                         JSON.stringify(this.shortcuts_config));
    //     console.log('Saved gamepad shortcuts keys for robot '+this.client.id_robot+', gamepad "'+this.gamepad.id+'":', this.shortcuts_config);
    // }

    // load_user_shortcuts(id_gamepad) {
    //     let cfg = localStorage.getItem('gamepad-keys:' + this.client.id_robot
    //                                     + ':' + id_gamepad);
    //     if (cfg) {
    //         try {
    //             cfg = JSON.parse(cfg);
    //         }
    //         catch {
    //             cfg = null;
    //         }
    //     }
    //     console.log('Loaded gamepad shortcuts keys for robot '+this.client.id_robot+', gamepad "'+id_gamepad+'":', cfg);
    //     return cfg;
    // }

    




        // // let transmitting = $('#gamepad_enabled').is(':checked');

        // if (!this.current_driver || !this.current_driver.config) {
        //     // wait for init
        //     return window.setTimeout(() => { this.run_loop(); }, this.loop_delay);
        // }

        // let msg_type = this.current_driver.msg_type;
        // let topic = this.current_driver.config.topic;

        // if (!this.client.topic_writers[topic]) {
        //     this.client.create_writer(topic, msg_type);
        // }

        // const gp = navigator.getGamepads()[this.gamepad.index];

        // let buttons = gp.buttons;
        // let axes = gp.axes;

        // let axes_debug = {};
        // for (let i = 0; i < axes.length; i++) {
        //     axes_debug[i] = axes[i]
        // }

        // let buttons_debug = {};
        // for (let i = 0; i < buttons.length; i++) {
        //     buttons_debug[i] = buttons[i].pressed;
        // }

        // $('#gamepad_debug_input').html('<b>Raw Axes:</b><br><div class="p">' + this.unquote(JSON.stringify(axes_debug, null, 4)) + '</div>' +
        //                                '<b>Raw Buttons:</b><br><div class="p">' + this.unquote(JSON.stringify(buttons_debug, null, 4)) + '</div>'
        //                                );

        // if (this.enabled) {
        //     let msg = this.current_driver.read(axes, buttons);

        //     if (this.client.topic_writers[topic].send(msg)) { // true when ready and written
        //         this.display_output(msg);
        //     }
        // } else if (!this.enabled && $('#gamepad').hasClass('open')) {
        //     let msg = this.current_driver.read(axes, buttons);
        //     this.display_output(msg);
        // }

        // if (this.editor_listening && $("#gamepad_shortcuts_input").is(":focus")) {

        //     for (let i = 0; i < buttons.length; i++) {

        //         if (buttons[i] && buttons[i].pressed) {
        //             console.log('Btn pressed: '+i+'; last=', this.last_buttons[i])
        //         }

        //         if (buttons[i] && buttons[i].pressed && (this.last_buttons[i] == undefined || !this.last_buttons[i])) {

        //             this.editor_listening = false;
        //             $('#gamepad_shortcuts_listen').removeClass('listening');
                    
        //             let pos = document.getElementById("gamepad_shortcuts_input").selectionStart;
        //             let curr_val = $('#gamepad_shortcuts_input').val();
        //             let insert = ''+i+'';
        //             let val = curr_val.slice(0,pos)+insert+curr_val.slice(pos)
        //             $('#gamepad_shortcuts_input').val(val);
        //             let new_pos = pos+insert.length;
                    
        //             document.getElementById('gamepad_shortcuts_input').setSelectionRange(new_pos, new_pos);
        //             break;
        //         }
        //     }

        // } 

        // for (let i = 0; i < buttons.length; i++) {
        //     if (buttons[i] && buttons[i].pressed && !this.last_buttons[i]) {
        //         if (this.shortcuts_config && this.shortcuts_config[i]) {
        //             this.handle_shortcut(this.shortcuts_config[i]);
        //         }
        //     }
        // }
        // this.last_buttons = [];
        // for (let i = 0; i < buttons.length; i++) {
        //     this.last_buttons.push(buttons[i].pressed);
        // }
    
        // window.setTimeout(() => { this.run_loop(); }, this.loop_delay);
    //}

    // update_output_info() {
    //     $('#gamepad_debug_output_label').html(' into '+this.current_driver.config.topic);
    //     $('#gamepad_debug_output B').html(this.current_driver.msg_type);
    // }

    // display_output(msg) {
    //     // this.update_output_info();
    //     $('#gamepad_debug_output .p').html(this.unquote(JSON.stringify(msg, null, 4)));
    // }

    // handle_shortcut = (cfg) => {
    //     console.log('handling gp shortcut', cfg);
    //     Handle_Shortcut(cfg, this.client);
    // }

    // init_writer (topic, msg_type) {



    //     this.dcs[topic] = this.client.create_dc(topic);

    //     return this.dcs[topic];
        // let subscription_data = {
        //     id_robot: this.id_robot,
        //     topics: []
        // };

        // if (!this.dcs[this.joy_topic])
        //     subscription_data.topics.push([ this.joy_topic, 1, this.joy_msg_type ])

        // if (!this.dcs[this.twist_topic])
        //     subscription_data.topics.push([ this.twist_topic, 1, this.twist_msg_type ])

        // if (!subscription_data.topics.length)
        //     return;

        // this.socket.emit('subcribe:write', subscription_data, (res) => {
        //     if (res['success']) {
        //         for (let i = 0; i < res['subscribed'].length; i++) {

        //             let topic = res['subscribed'][i][0];
        //             let id = res['subscribed'][i][1];
        //             let protocol = res['subscribed'][i][2];

        //             console.log('Making local DC for '+topic+', id='+id+', protocol='+protocol)
        //             this.dcs[topic] = pc.createDataChannel(topic, {
        //                 negotiated: true,
        //                 ordered: false,
        //                 maxRetransmits: null,
        //                 id:id
        //             });

        //             this.dcs[topic].addEventListener('open', (ev)=> {
        //                 console.info('DC '+topic+'/W opened', this.dcs[topic])
        //             });
        //             this.dcs[topic].addEventListener('close', (ev)=> {
        //                 console.info('DC '+topic+'/W closed')
        //                 delete this.dcs[topic];
        //             });
        //             this.dcs[topic].addEventListener('error', (ev)=> {
        //                 console.error('DC '+topic+'/W error', ev)
        //                 delete this.dcs[topic];
        //             });
        //             this.dcs[topic].addEventListener('message', (ev)=> {
        //                 console.warn('DC '+topic+'/W message!!', ev); //this should not be as we use separate r/w channels
        //             });
        //         }
        //     } else {
        //         console.warn('Error setting up gamepad publisher: ', res);
        //     }
        // });
    // }



    // clearProducers() {
        // for (const topic of Object.keys(this.dcs)) {
        //     if (this.dcs[topic])
        //         this.dcs[topic].close();
        // }
        // this.dcs = {}
    // }


    

    // SampleLatency(decoded) {
    //     let sec = decoded.header.stamp.sec
    //     let nanosec = decoded.header.stamp.nanosec
    //     let msg_ms = (sec * 1000) + (nanosec / 1000000);
    //     let now_ms = Date.now(); //window.performance.now()
    //     let lat = now_ms - msg_ms;
    //     // let sec = Math.floor(now_ms / 1000)
    //     // let nanosec = (now_ms - sec*1000) * 1000000

    //     $('#gamepad_latency').html(lat+' ms')
    //     // console.log('Sampling joy lag: ', )
    // }

    // capture_gamepad_input(buttons, axes) {
    //     if (!this.capturing_gamepad_input) {
    //         return;
    //     }

    //     let something_pressed = false;
    //     for (let i = 0; i < buttons.length; i++) {
    //         if (buttons[i] && buttons[i].pressed) {
    //             something_pressed = true;
    //             if (this.captured_gamepad_input.indexOf(i) === -1) {
    //                 this.captured_gamepad_input.push(i);
    //             }
    //         }
    //     }
    //     if (something_pressed) {
    //         for (let i = 0; i < this.captured_gamepad_input.length; i++) {
    //             let btn = this.captured_gamepad_input[i];
    //             if (!buttons[btn] || !buttons[btn].pressed) {
    //                 this.captured_gamepad_input.splice(i, 1);
    //                 i--;
    //             }
    //         }
    //     }

    //     $('#current-key').html(this.captured_gamepad_input.join(' + '));
    // }


    // MarkMappedServiceButtons() {
    //     if (!this.gamepad_service_mapping)
    //         return;

    //     $('.service_button').removeClass('mapped');
    //     $('.service_button').attr('title');
    //     for (const [service_name, service_mapping] of Object.entries(this.gamepad_service_mapping)) {
    //         for (const [btn_name, btns_config] of Object.entries(service_mapping)) {
    //             console.log('MARKING MAPPED: ', service_name, btn_name, $('.service_button[data-service="'+service_name+'"][data-name="'+btn_name+'"]'))
    //             let btns_print = [];
    //             for (let i = 0; i < btns_config.btns_cond.length; i++) {
    //                 let b = btns_config.btns_cond[i];
    //                 btns_print.push('['+b+']');
    //             }
    //             $('.service_button[data-service="'+service_name+'"][data-name="'+btn_name+'"]')
    //                 .addClass('mapped')
    //                 .attr('title', 'Mapped to gamepad button(s): '+btns_print.join(' + '));
    //         }
    //     }
    // }

    // static SaveGamepadServiceMapping(id_robot) {

    //     MarkMappedServiceButtons();

    //     if (typeof(Storage) === "undefined") {
    //         console.warn('No Web Storage support, cannot save gamepad mapping');
    //         return;
    //     }

    //     let data = [];
    //     for (const [service_name, service_mapping] of Object.entries(gamepad_service_mapping)) {
    //         for (const [btn_name, btns_config] of Object.entries(service_mapping)) {
    //             let service_data = {
    //                 service_name: service_name,
    //                 btn_name: btn_name,
    //                 btns_cond: btns_config.btns_cond
    //             }
    //             data.push(service_data);
    //         }
    //     }
    //     let val = JSON.stringify(data);
    //     localStorage.setItem('gamepad_service_mapping:'+id_robot, val);
    //     console.log('Saved Gamepad Service Mapping for robot '+id_robot+':', val);
    // }

    // load_gamepad_service_mapping() {
    //     if (typeof(Storage) === "undefined") {
    //         console.warn('No Web Storage support, cannot load gamepad mapping');
    //         return;
    //     }

    //     console.log('Loading Gamepad Service Mapping for robot '+this.client.id_robot+'...');

    //     this.gamepad_service_mapping = {};
    //     let json = localStorage.getItem('gamepad_service_mapping:'+this.client.id_robot);
    //     if (!json)
    //         return;
    //     let val = JSON.parse(json);

    //     for (let i = 0; i < val.length; i++) {
    //         let service_data = val[i];
    //         if (!this.gamepad_service_mapping[service_data.service_name])
    //             this.gamepad_service_mapping[service_data.service_name] = {};
    //         this.gamepad_service_mapping[service_data.service_name][service_data.btn_name] = {
    //             btns_cond: service_data.btns_cond,
    //             needs_reset: false
    //         };
    //     }
    //     console.log('Loaded Gamepad Service Mapping:', val, this.gamepad_service_mapping);
    // }

    // MapServiceButton(button, id_robot) {

    //     let service_name = $(button).attr('data-service');
    //     let btn_name = $(button).attr('data-name');
    //     console.warn('Mapping '+service_name+' => ' + btn_name +' ...');

    //     $('#mapping-confirmation').attr('title', 'Mapping '+service_name+':'+btn_name);
    //     $('#mapping-confirmation').html('Press a gamepad button or combination...<br><br><span id="current-key"></span>');
    //     this.captured_gamepad_input = [];
    //     this.capturing_gamepad_input = true;
    //     $( "#mapping-confirmation" ).dialog({
    //         resizable: false,
    //         height: "auto",
    //         width: 400,
    //         modal: true,
    //         buttons: {
    //           Clear: function() {
    //             this.captured_gamepad_input = [];
    //             $('#current-key').html('');
    //             //$( this ).dialog( "close" );
    //           },
    //           Cancel: function() {
    //             this.capturing_gamepad_input = false;
    //             $( this ).dialog( "close" );
    //           },
    //           Save: function() {
    //             capturing_gamepad_input = false;
    //             if (!gamepad_service_mapping[service_name])
    //                 gamepad_service_mapping[service_name] = {};
    //             if (!gamepad_service_mapping[service_name][btn_name])
    //                 gamepad_service_mapping[service_name][btn_name] = { };

    //             if (captured_gamepad_input.length > 0) {
    //                 gamepad_service_mapping[service_name][btn_name]['btns_cond'] = captured_gamepad_input;
    //                 captured_gamepad_input = [];
    //                 gamepad_service_mapping[service_name][btn_name]['needs_reset'] = true;
    //             } else {
    //                 delete gamepad_service_mapping[service_name][btn_name];
    //                 if (Object.keys(gamepad_service_mapping[service_name]).length == 0)
    //                     delete gamepad_service_mapping[service_name];
    //             }


    //             //console.log('Mapping saved: ', gamepad_service_mapping);
    //             $( this ).dialog( "close" );
    //             $('#service_controls.setting_shortcuts').removeClass('setting_shortcuts');
    //             $('#services_gamepad_mapping_toggle').html('[shortcuts]');

    //             SaveGamepadServiceMapping(id_robot);
    //           }
    //         }
    //     });
    // }
}