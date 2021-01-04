import sys
import datetime
import os


class Logger:
    log = ""
    log_folder = os.path.join(os.getcwd(), "Logs")
    script_name = ""
    add_logs_to_arc_messages = False

    def __init__(self, add_logs_to_arc_messages=False):
        self.addLogsToArcpyMessages = add_logs_to_arc_messages
        now = datetime.datetime.now()
        today = now.strftime("%Y-%m-%d")
        time = now.strftime("%I:%M %p")
        self.script_name = os.path.split(sys.argv[0])[1]
        self.log = f"{self.script_name} || {today} : {time} || {os.getenv('COMPUTERNAME')}"

        if not os.path.exists(self.log_folder):
            os.mkdir(self.log_folder)

        self.log_file = os.path.join(self.log_folder, today + ".txt")
        print("Logger Initialized: " + self.log)

    def log_msg(self, msg, print_msg=True):
        """
        logs a message and prints it to the screen
        """
        time = datetime.datetime.now().strftime("%I:%M %p")
        self.log = "{0}\n{1} | {2}".format(self.log, time, msg)
        if print_msg:
            print(msg)

        if self.add_logs_to_arc_messages:
            from arcpy import AddMessage
            AddMessage(msg)

    def log_GP_msg(self, print_msg=True):
        """
        logs the arcpy messages and prints them to the screen
        """
        from arcpy import GetMessages
        msgs = GetMessages()
        try:
            self.log_msg(msgs, print_msg)
        except:
            self.log_msg("error getting arcpy message", print_msg)

    def write_log_to_file(self):
        """
        writes the log to a
        """
        if not os.path.exists(self.log_folder):
            os.mkdir(self.log_folder)

        with open(self.log_file, mode="a") as f:
            f.write("\n\n" + self.log)

    def log_error(self):
        """
        gets traceback info and logs it
        """
        # got from http://webhelp.esri.com/arcgisdesktop/9.3/index.cfm?TopicName=Error_handling_with_Python
        import traceback

        self.log_msg("ERROR!!!")
        err_msg = traceback.format_exc()
        self.log_msg(err_msg)
        return err_msg
