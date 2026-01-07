import serial
import sys

ser = serial.Serial(port=None)
ser.baudrate = 115200
ser.port = 'COM4'
ser.exclusive = True
ser.timeout = 2
ser.rts = False
ser.dtr = False
ser.open()
#ser.rts = False
print(ser)
ser.close()
sys.exit()