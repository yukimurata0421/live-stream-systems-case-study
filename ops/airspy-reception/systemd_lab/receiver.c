/* Isolated systemd fixture: synthetic reception, constant argv, no USB or RF. */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>

static void write_file(const char *path, const char *s) {
    char tmp[512]; snprintf(tmp, sizeof tmp, "%s.tmp", path);
    FILE *f=fopen(tmp,"w"); if(!f) exit(70);
    fputs(s,f); fflush(f); fsync(fileno(f)); fclose(f);
    if(rename(tmp,path)) exit(71);
}
int main(void) {
    int starts=0; FILE *f=fopen("/var/lib/lab/starts","r");
    if(f) { if(fscanf(f,"%d",&starts)!=1) return 72; fclose(f); } starts++;
    char s[4096], mode[64]="success";
    f=fopen("/control/mode","r"); if(f) { if(fscanf(f,"%63s",mode)!=1) return 72; fclose(f); }
    snprintf(s,sizeof s,"%d\n",starts); write_file("/var/lib/lab/starts",s);
    printf("LAB_RECEIVER_START count=%d pid=%d mode=%s\n",starts,getpid(),mode); fflush(stdout);
    if(starts==2 && (!strcmp(mode,"timeout") || !strcmp(mode,"monitor_crash"))) signal(SIGTERM,SIG_IGN);
    if(starts>=3 && !strcmp(mode,"job_failure")) { sleep(1); return 42; }
    int healthy=starts==1 || (starts>=3 && (!strcmp(mode,"success") || !strcmp(mode,"gain_change")));
    double gain=16;
    f=fopen("/etc/default/airspy_adsb","r");
    if(!f || fscanf(f,"GAIN=\"%lf\"",&gain)!=1) return 73;
    fclose(f);
    double period=1; f=fopen("/control/period","r"); if(f) { if(fscanf(f,"%lf",&period)!=1) return 72; fclose(f); }
    mkdir("/run/airspy_adsb",0755);
    while(1) {
        struct timespec ts; clock_gettime(CLOCK_REALTIME,&ts);
        double now=ts.tv_sec+ts.tv_nsec/1e9;
        int n=snprintf(s,sizeof s,"{\"now\":%.6f,\"gain\":%.1f,\"lost_buffers\":0,\"df_counts\":[",now,gain);
        for(int i=0;i<32;i++) n+=snprintf(s+n,sizeof(s)-n,"%s%d",i?",":"",i==17?(healthy?6000:120):0);
        snprintf(s+n,sizeof(s)-n,"]}\n"); write_file("/run/airspy_adsb/stats.json",s);
        write_file("/run/receiver-healthy",healthy?"1":"0");
        usleep((useconds_t)(period*1e6));
    }
}
