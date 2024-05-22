export class InputDriver {

    constructor(gamepad_controller) {
        // this.id = id;
        // this.label = label;
        // this.config = null;
        this.gamepad_controller = gamepad_controller;

        this.output_topic = '/user_input'; //override this
        this.client = gamepad_controller.client;
        this.output = null;
        this.topic_writer = null;

        this.error_label = null;
        this.error_message = null;

        this.saved_state = null;
    }

    set_config(cfg) {
        if (cfg && cfg.output_topic) {
            this.output_topic = cfg.output_topic;
        }
        this.setup_writer();
    }

    set_saved_state() {
        this.saved_state = {
            msg_type: this.msg_type,
            output_topic: this.output_topic
        }
    }

    check_saved() {
        if (!this.saved_state)
            return false;
        if (this.saved_state.msg_type != this.msg_type)
            return false;
        if (this.saved_state.output_topic != this.output_topic)
            return false;

        return true; // saved
    }

    setup_writer() {
        if (!this.msg_type) {
            console.error('msg_type not defined in InputDriver subclass')
            return;
        }
        let err = {};
        this.topic_writer = this.client.get_writer(this.output_topic, this.msg_type, null, err);
        this.error_message = err.message;
        this.handle_error_message();
    }

    handle_error_message() {
        if (this.error_message && this.error_label) {
            this.error_label
                .html(this.error_message)
                .css('display', 'block');
        } else if (!this.error_message && this.error_label) {
            this.error_label
                .empty()
                .css('display', 'none');
        }
    }


    get_header() {
        let now_ms = Date.now(); //window.performance.now()
        let sec = Math.floor(now_ms / 1000);
        let nanosec = (now_ms - sec*1000) * 1000000;
        return {
            stamp: {
                sec: sec,
                nanosec: nanosec
            },
            frame_id: 'gamepad'
        }
    }

    // override this for more input config options
    make_cofig_inputs() {
        let lines = [];

        // one output topic by default
        let line_topic = $('<div class="line"><span class="label">Output topic:</span></div>');
        let inp_topic = $('<input type="text" inputmode="url" autocomplete="off" value="' + this.output_topic + '"/>');
        let msg_type_hint = $('<span class="comment">'+this.msg_type+'</span>');
        this.error_label = $('<span class="driver-error"></span>');
        
        inp_topic.appendTo(line_topic);
        msg_type_hint.appendTo(line_topic);
    
        let that = this;
        inp_topic.change((ev)=>{
            that.output_topic = $(ev.target).val();
            // console.log('Driver output topic is: '+that.output_topic);
            that.setup_writer();
            that.gamepad_controller.check_profile_saved(that.gamepad_controller.current_gamepad, that.gamepad_controller.current_gamepad.current_profile);
        });

        lines.push(line_topic);
        lines.push(this.error_label);

        this.handle_error_message();

        return lines;
    }

    display_output(el, transmitting) {
        el.html('Message: <b>'+this.msg_type+'</b><br>'
                + 'Topic: <b>'+this.output_topic+'</b> '+ (transmitting ? '(transmitting)' : '(not transmitting)')  +'<br><br>'
                + JSON.stringify(this.output, null, 4));
    }

    can_transmit() {
        return this.topic_writer !== null
               && this.topic_writer !== undefined
               && this.topic_writer !== false;
    }

    transmit() {
        if (!this.output)
            return;
        if (!this.topic_writer) {
            // console.log('driver writer not ready for '+this.output_topic)
            return;
        }
        if (!this.topic_writer.send(this.output)) { // true when ready and written
            // this.display_output(msg);
            console.error('Gamepad driver writer failed writing into topic (warming up?)');
        }
    }
}